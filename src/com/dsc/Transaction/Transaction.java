package com.dsc.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

/**
 * A simple Software Transactional Memory (STM) implementing the TL2 algorithm:
 *
 * <ol>
 * <li><b>Global version clock</b> {@link #globalClock} (a long, so it never overflows). A transaction
 *     records its snapshot version (readVersion) when it starts.</li>
 * <li><b>One versioned lock per variable</b>: the state of a {@link TVar} is an immutable
 *     (value, version, lock owner) triple, locked and written back with a single CAS.</li>
 * <li><b>Read</b>: if the variable is locked (someone is committing it) or its version is
 *     &gt; readVersion, abort and retry. The transaction body therefore always sees a consistent
 *     snapshot (opacity).</li>
 * <li><b>Write</b>: goes only into the transaction's private write set, invisible to other threads.</li>
 * <li><b>Commit</b>: read-only transactions succeed immediately (no locks at all). Write transactions:
 *     (1) CAS-lock every variable in the write set (abort if a lock is taken, so no deadlock);
 *     (2) writeVersion = ++globalClock;
 *     (3) validate that nothing in the read set has changed;
 *     (4) write the new values back and unlock with writeVersion.
 *     Transactions that write disjoint variables commit fully in parallel.</li>
 * <li><b>Rollback</b>: just discard the write set; nothing reaches shared memory before commit.</li>
 * </ol>
 *
 * Also supported:
 * <ul>
 * <li>{@link #retry()}: block when a condition does not hold, and rerun once another transaction
 *     modifies a variable that was read (enough to build blocking queues, etc.).</li>
 * <li>{@link #orElse}: if the first branch retries, undo its writes and run the second branch.</li>
 * <li>{@link #afterCommit}: register side effects (I/O, logging, messaging) that run exactly once,
 *     only after the transaction really commits.</li>
 * <li>Nested transactions are flattened (flat nesting): an inner atomic joins the outer transaction.</li>
 * </ul>
 *
 * <pre>
 * Transaction.atomic(() -&gt; {
 *     a.put(a.get() - 10);
 *     b.put(b.get() + 10);
 *     Transaction.afterCommit(() -&gt; System.out.println("transfer done"));
 * });
 * </pre>
 */
public final class Transaction {

	/** Global version clock, incremented by every committing write transaction */
	private static final AtomicLong globalClock = new AtomicLong(0);
	private static final ThreadLocal<Transaction> current = new ThreadLocal<Transaction>();
	private static final AtomicLong idGenerator = new AtomicLong(0);

	private static final AtomicLong commitCount = new AtomicLong();
	private static final AtomicLong abortCount = new AtomicLong();
	private static final AtomicLong retryCount = new AtomicLong();

	private final long id;
	/** Snapshot version of this transaction */
	private final long readVersion;
	/** Read set: variables that were read (validated at commit, and waited on by retry) */
	private final Set<TVar<?>> readSet = Collections.newSetFromMap(new IdentityHashMap<TVar<?>, Boolean>());
	/** Write set: variable -> new value (written back on commit). Replaced as a whole when orElse rolls back a branch */
	private Map<TVar<?>, Object> writeSet = new IdentityHashMap<TVar<?>, Object>();
	/** Side effects to run after a successful commit, created lazily */
	private List<Runnable> afterCommitActions;
	/** Set once a conflict is detected, so the transaction cannot commit if application code swallows ConflictException */
	private boolean doomed = false;

	private Transaction() {
		this.id = idGenerator.incrementAndGet();
		this.readVersion = globalClock.get();
	}

	// ================================================================== Public API

	/**
	 * Runs body in a transaction and returns its result. Conflicts are retried automatically and
	 * retry() blocks, until the transaction commits.
	 */
	public static <T> T atomic(Supplier<T> body) {
		if (current.get() != null) {
			// Flat nesting: just run inside the outer transaction
			return body.get();
		}
		int attempt = 0;
		while (true) {
			Transaction tx = new Transaction();
			current.set(tx);
			T result = null;
			boolean committed = false, conflict = false;
			try {
				result = body.get();
				tx.commit();
				committed = true;
			} catch (ConflictException e) {
				conflict = true;
			} catch (RetrySignal e) {
				// Handled below by blocking
			} finally {
				// Application exceptions also end up here: the write set is simply dropped (a rollback)
				// and the exception propagates unchanged
				current.remove();
			}

			if (committed) {
				commitCount.incrementAndGet();
				tx.runAfterCommitActions();
				return result;
			} else if (conflict) {
				abortCount.incrementAndGet();
				backoff(++attempt);
			} else {
				retryCount.incrementAndGet();
				tx.awaitChange();
				attempt = 0;
			}
		}
	}

	/** Variant without a return value. */
	public static void atomic(final Runnable body) {
		atomic(new Supplier<Void>() {
			@Override
			public Void get() {
				body.run();
				return null;
			}
		});
	}

	/**
	 * Abandons this run and blocks until some variable this transaction read is modified by another
	 * transaction, then reruns from the start.
	 * Typical use: {@code if (queue.isEmpty()) Transaction.retry();}
	 * <p>
	 * If there is an enclosing {@link #orElse}, its other branch is tried first.
	 */
	public static void retry() {
		Transaction tx = requireCurrent("retry()");
		tx.checkNotDoomed();
		throw RetrySignal.INSTANCE;
	}

	/**
	 * Runs first; if first calls {@link #retry()}, undoes all of its writes (and the afterCommit
	 * actions it registered) and runs second instead.
	 * If second also retries, the whole transaction retries (waiting for any variable read by either branch).
	 * Called outside a transaction, it starts one automatically.
	 */
	public static <T> T orElse(final Supplier<T> first, final Supplier<T> second) {
		Transaction tx = current.get();
		if (tx == null) {
			return atomic(new Supplier<T>() {
				@Override
				public T get() {
					return orElse(first, second);
				}
			});
		}
		tx.checkNotDoomed();
		Map<TVar<?>, Object> savedWrites = new IdentityHashMap<TVar<?>, Object>(tx.writeSet);
		int savedActions = tx.afterCommitActions == null ? 0 : tx.afterCommitActions.size();
		try {
			return first.get();
		} catch (RetrySignal r) {
			// Roll back the writes of the first branch. Its read set is kept, so that if both branches
			// retry we wait on every relevant variable
			tx.writeSet = savedWrites;
			if (tx.afterCommitActions != null) {
				tx.afterCommitActions.subList(savedActions, tx.afterCommitActions.size()).clear();
			}
			return second.get();
		}
	}

	/** Variant without a return value. */
	public static void orElse(final Runnable first, final Runnable second) {
		orElse(new Supplier<Void>() {
			@Override
			public Void get() {
				first.run();
				return null;
			}
		}, new Supplier<Void>() {
			@Override
			public Void get() {
				second.run();
				return null;
			}
		});
	}

	/**
	 * Registers a side effect that runs exactly once after the current (outermost) transaction commits.
	 * It is discarded if the transaction aborts, reruns or throws.
	 * The transaction body may run several times, so irreversible operations such as I/O, logging and
	 * sending messages belong here.
	 * <p>
	 * Called outside a transaction, the action runs immediately. Actions run in registration order;
	 * one throwing does not stop the others, and the first exception is rethrown after all of them have
	 * run (the transaction has already committed by then).
	 */
	public static void afterCommit(Runnable action) {
		Transaction tx = current.get();
		if (tx == null) {
			action.run();
			return;
		}
		tx.checkNotDoomed();
		if (tx.afterCommitActions == null) {
			tx.afterCommitActions = new ArrayList<Runnable>();
		}
		tx.afterCommitActions.add(action);
	}

	/** The transaction running on the current thread, or null if there is none. */
	public static Transaction getCurrent() {
		return current.get();
	}

	public long getId() {
		return id;
	}

	public long getReadVersion() {
		return readVersion;
	}

	public static long getGlobalVersion() {
		return globalClock.get();
	}

	public static long getCommitCount() {
		return commitCount.get();
	}

	public static long getAbortCount() {
		return abortCount.get();
	}

	/** Number of times a transaction blocked because of retry() */
	public static long getRetryCount() {
		return retryCount.get();
	}

	/** Thrown when a thread is interrupted while blocked in retry(). The thread's interrupt flag is preserved. */
	public static final class RetryInterruptedException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		RetryInterruptedException() {
			super("interrupted while blocked in Transaction.retry()");
		}
	}

	// ================================================================== Called by TVar

	@SuppressWarnings("unchecked")
	static <T> T read(TVar<T> var) {
		Transaction tx = current.get();
		if (tx == null) {
			return (T) var.slot.get().value;
		}
		tx.checkNotDoomed();
		// Read your own writes
		if (tx.writeSet.containsKey(var)) {
			return (T) tx.writeSet.get(var);
		}
		TVar.Slot s = var.slot.get(); // value, version and lock state read atomically in one go
		if (s.owner != null || s.version > tx.readVersion) {
			// Being committed by someone else, or changed after we started: the snapshot is no longer consistent
			tx.abort();
		}
		tx.readSet.add(var);
		return (T) s.value;
	}

	static <T> void write(final TVar<T> var, final T value) {
		Transaction tx = current.get();
		if (tx == null) {
			// Write outside a transaction: wrap it in a single-write transaction
			atomic(new Runnable() {
				@Override
				public void run() {
					write(var, value);
				}
			});
			return;
		}
		tx.checkNotDoomed();
		tx.writeSet.put(var, value);
	}

	// ================================================================== Internals

	private void commit() {
		checkNotDoomed();
		if (writeSet.isEmpty()) {
			// Read-only: every read already checked version <= readVersion and unlocked, so the snapshot is consistent
			return;
		}
		List<TVar<?>> locked = new ArrayList<TVar<?>>(writeSet.size());
		boolean success = false;
		try {
			// (1) Lock the write set. Abort instead of waiting when a lock is taken, so there is no deadlock
			for (TVar<?> var : writeSet.keySet()) {
				TVar.Slot s = var.slot.get();
				if (s.owner != null || !var.slot.compareAndSet(s, new TVar.Slot(s.value, s.version, this))) {
					abort();
				}
				locked.add(var);
			}
			// (2) Get the commit version
			long writeVersion = globalClock.incrementAndGet();
			// (3) Validate the read set. writeVersion == readVersion + 1 means nobody else committed
			//     in between, so validation can be skipped
			if (writeVersion != readVersion + 1) {
				for (TVar<?> var : readSet) {
					TVar.Slot s = var.slot.get();
					if ((s.owner != null && s.owner != this) || s.version > readVersion) {
						abort();
					}
				}
			}
			// (4) Write back the new values, unlocking at the same time
			for (Map.Entry<TVar<?>, Object> e : writeSet.entrySet()) {
				e.getKey().slot.set(new TVar.Slot(e.getValue(), writeVersion, null));
			}
			success = true;
		} finally {
			if (!success) {
				// Failure: restore the old value and version, releasing the locks we acquired
				for (TVar<?> var : locked) {
					TVar.Slot s = var.slot.get();
					var.slot.set(new TVar.Slot(s.value, s.version, null));
				}
			}
		}
		// Wake up threads blocked in retry() on these variables
		for (TVar<?> var : writeSet.keySet()) {
			var.signalWaiters();
		}
	}

	/**
	 * After retry(): register as a waiter on every variable in the read set and block until any of them
	 * is modified by a commit. We register before checking versions, and committers write values before
	 * waking waiters, so no wake-up can be lost.
	 */
	private void awaitChange() {
		if (readSet.isEmpty()) {
			throw new IllegalStateException("retry() called before reading any transactional variable; it would block forever");
		}
		Waiter waiter = new Waiter(Thread.currentThread());
		for (TVar<?> var : readSet) {
			var.waiters.add(waiter);
		}
		try {
			for (TVar<?> var : readSet) {
				if (var.slot.get().version > readVersion) {
					return; // Already changed while we were registering: rerun right away
				}
			}
			while (!waiter.signaled) {
				LockSupport.park(this);
				if (Thread.interrupted()) {
					Thread.currentThread().interrupt();
					throw new RetryInterruptedException();
				}
			}
		} finally {
			for (TVar<?> var : readSet) {
				var.waiters.remove(waiter);
			}
		}
	}

	private void runAfterCommitActions() {
		if (afterCommitActions == null) {
			return;
		}
		RuntimeException first = null;
		for (Runnable action : afterCommitActions) {
			try {
				action.run();
			} catch (RuntimeException e) {
				if (first == null) {
					first = e;
				} else {
					first.addSuppressed(e);
				}
			}
		}
		if (first != null) {
			throw first;
		}
	}

	private void abort() {
		doomed = true;
		throw ConflictException.INSTANCE;
	}

	private void checkNotDoomed() {
		if (doomed) {
			throw ConflictException.INSTANCE;
		}
	}

	private static Transaction requireCurrent(String op) {
		Transaction tx = current.get();
		if (tx == null) {
			throw new IllegalStateException(op + " can only be called inside a transaction");
		}
		return tx;
	}

	/** Randomized exponential backoff to reduce livelock */
	private static void backoff(int attempt) {
		if (attempt <= 2) {
			Thread.yield();
			return;
		}
		int maxMicros = 1 << Math.min(attempt, 10);
		LockSupport.parkNanos(ThreadLocalRandom.current().nextInt(maxMicros) * 1000L);
	}

	/** A thread blocked in retry() */
	static final class Waiter {
		private final Thread thread;
		volatile boolean signaled = false;

		Waiter(Thread thread) {
			this.thread = thread;
		}

		void signal() {
			signaled = true;
			LockSupport.unpark(thread);
		}
	}
}
