package com.dsc.Transaction;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Low-level implementation of a transactional variable.
 * <p>
 * The state lives in an immutable {@link Slot} (value, version, lock owner) that is replaced atomically
 * through an {@link AtomicReference}. This is the same idea as {@code AtomicStampedReference} (which is
 * itself an AtomicReference&lt;Pair&gt; internally), except that the version is a long (no overflow) and
 * there is an extra "lock owner" field, which gives every variable its own versioned lock.
 * <p>
 * The internal fields are visible only to {@link Transaction} (same package); application code
 * accesses the variable through get/put only.
 */
public class TVar<T> {

	/** Immutable snapshot: owner != null means a transaction is committing it (locked). */
	static final class Slot {
		final Object value;
		final long version;
		final Transaction owner;

		Slot(Object value, long version, Transaction owner) {
			this.value = value;
			this.version = version;
			this.owner = owner;
		}
	}

	final AtomicReference<Slot> slot;

	/** Threads blocked in retry() that are waiting for this variable to change. */
	final Set<Transaction.Waiter> waiters = ConcurrentHashMap.newKeySet();

	protected TVar(T initialValue) {
		this.slot = new AtomicReference<Slot>(new Slot(initialValue, 0L, null));
	}

	/** Reads the value (transactional read inside a transaction, latest committed value outside). */
	public T get() {
		return Transaction.read(this);
	}

	/**
	 * Writes the value (inside a transaction it only goes to the write set and takes effect on commit;
	 * outside a transaction it is wrapped in a small transaction automatically).
	 */
	public void put(T newValue) {
		Transaction.write(this, newValue);
	}

	/** Version of the last commit, for debugging/observation only. */
	public long getVersion() {
		return slot.get().version;
	}

	void signalWaiters() {
		if (waiters.isEmpty()) {
			return;
		}
		for (Transaction.Waiter w : waiters) {
			waiters.remove(w);
			w.signal();
		}
	}

	@Override
	public String toString() {
		Slot s = slot.get();
		return getClass().getSimpleName() + "[" + s.value + "@v" + s.version + (s.owner != null ? ",locked" : "") + "]";
	}
}
