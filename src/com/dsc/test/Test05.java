package com.dsc.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.dsc.Atomic.AtomicStamp;
import com.dsc.Transaction.Transaction;

/**
 * Tests for retry / orElse / afterCommit, and for the parallel commits enabled by per-variable locks.
 */
public class Test05 {

	/** Bounded blocking queue built on retry(): put blocks when full, take blocks when empty, no wait/notify */
	static final class TQueue {
		final AtomicStamp<List<Integer>> items = new AtomicStamp<List<Integer>>(Collections.<Integer>emptyList());
		final int capacity;

		TQueue(int capacity) {
			this.capacity = capacity;
		}

		void put(final int x) {
			Transaction.atomic(() -> {
				List<Integer> l = items.get();
				if (l.size() >= capacity) {
					Transaction.retry();
				}
				List<Integer> n = new ArrayList<Integer>(l); // treat the list as an immutable value: copy on every change
				n.add(x);
				items.put(n);
			});
		}

		int take() {
			return Transaction.atomic(() -> {
				List<Integer> l = items.get();
				if (l.isEmpty()) {
					Transaction.retry();
				}
				items.put(new ArrayList<Integer>(l.subList(1, l.size())));
				return l.get(0);
			});
		}
	}

	public static void main(String[] args) throws Exception {
		producerConsumer();
		orElseChoosesAvailableQueue();
		orElseRollsBackFirstBranch();
		afterCommitRunsExactlyOnce();
		retryWithoutReadsFails();
		retryIsInterruptible();
		disjointWritesRunInParallel();
		System.out.println("Test05 OK  commits=" + Transaction.getCommitCount() + " aborts="
				+ Transaction.getAbortCount() + " retries=" + Transaction.getRetryCount());
	}

	static void producerConsumer() throws InterruptedException {
		final TQueue q = new TQueue(4);
		final int producers = 4, consumers = 4, perThread = 5_000;
		final AtomicLong consumedSum = new AtomicLong();
		List<Thread> ts = new ArrayList<Thread>();
		for (int p = 0; p < producers; p++) {
			ts.add(new Thread(() -> {
				for (int i = 1; i <= perThread; i++) q.put(i);
			}));
		}
		for (int c = 0; c < consumers; c++) {
			ts.add(new Thread(() -> {
				for (int i = 0; i < perThread; i++) consumedSum.addAndGet(q.take());
			}));
		}
		long retriesBefore = Transaction.getRetryCount();
		for (Thread t : ts) t.start();
		for (Thread t : ts) t.join();
		long expected = (long) producers * perThread * (perThread + 1) / 2;
		check(consumedSum.get() == expected, "sum=" + consumedSum.get() + " expected=" + expected);
		check(q.items.get().isEmpty(), "queue should be empty");
		System.out.println("producer/consumer OK: sum=" + consumedSum.get()
				+ " blocked=" + (Transaction.getRetryCount() - retriesBefore) + " times");
	}

	static void orElseChoosesAvailableQueue() throws InterruptedException {
		final TQueue q1 = new TQueue(10), q2 = new TQueue(10);
		final AtomicInteger got = new AtomicInteger(-1);
		Thread consumer = new Thread(() -> got.set(Transaction.orElse(() -> q1.take(), () -> q2.take())));
		consumer.start();
		Thread.sleep(200);
		check(consumer.isAlive() && consumer.getState() == Thread.State.WAITING,
				"should block while both queues are empty, state=" + consumer.getState());
		q2.put(42); // writing q2 should wake it up
		consumer.join(2000);
		check(!consumer.isAlive() && got.get() == 42, "orElse should take 42 from q2, got=" + got.get());
		System.out.println("orElse wake-up OK: got " + got.get());
	}

	static void orElseRollsBackFirstBranch() {
		final AtomicStamp<Integer> x = new AtomicStamp<Integer>(1);
		final AtomicInteger hooks = new AtomicInteger();
		int r = Transaction.atomic(() -> Transaction.orElse(() -> {
			x.put(99);
			Transaction.afterCommit(() -> hooks.incrementAndGet()); // should be undone together with the branch
			Transaction.retry();
			return -1;
		}, () -> x.get()));
		check(r == 1 && x.get() == 1 && hooks.get() == 0, "r=" + r + " x=" + x.get() + " hooks=" + hooks.get());
		System.out.println("orElse rollback OK");
	}

	static void afterCommitRunsExactlyOnce() throws InterruptedException {
		final AtomicStamp<Integer> counter = new AtomicStamp<Integer>(0);
		final AtomicInteger bodyRuns = new AtomicInteger(), hookRuns = new AtomicInteger();
		final int threads = 8, perThread = 10_000;
		Thread[] ts = new Thread[threads];
		for (int t = 0; t < threads; t++) {
			ts[t] = new Thread(() -> {
				for (int i = 0; i < perThread; i++) {
					Transaction.atomic(() -> {
						bodyRuns.incrementAndGet();
						counter.put(counter.get() + 1);
						Transaction.afterCommit(() -> hookRuns.incrementAndGet());
					});
				}
			});
			ts[t].start();
		}
		for (Thread t : ts) t.join();
		int expected = threads * perThread;
		check(counter.get() == expected && hookRuns.get() == expected,
				"counter=" + counter.get() + " hooks=" + hookRuns.get());

		// Not run when an application exception rolls the transaction back
		try {
			Transaction.atomic(() -> {
				Transaction.afterCommit(() -> hookRuns.incrementAndGet());
				throw new IllegalStateException("fail");
			});
		} catch (IllegalStateException expectedEx) {
			// ok
		}
		check(hookRuns.get() == expected, "afterCommit of a failed transaction must not run");
		System.out.println("afterCommit OK: body ran " + bodyRuns.get() + " times (including conflict reruns), side effect ran exactly "
				+ hookRuns.get() + " times");
	}

	static void retryWithoutReadsFails() {
		try {
			Transaction.atomic(() -> Transaction.retry());
			throw new AssertionError("expected IllegalStateException");
		} catch (IllegalStateException expected) {
			System.out.println("retry without reads OK: " + expected.getMessage());
		}
	}

	static void retryIsInterruptible() throws InterruptedException {
		final TQueue q = new TQueue(1);
		final AtomicInteger outcome = new AtomicInteger();
		Thread t = new Thread(() -> {
			try {
				q.take();
			} catch (Transaction.RetryInterruptedException e) {
				outcome.set(Thread.currentThread().isInterrupted() ? 1 : 2);
			}
		});
		t.start();
		Thread.sleep(100);
		t.interrupt();
		t.join(2000);
		check(outcome.get() == 1, "expected RetryInterruptedException with the interrupt flag kept, outcome=" + outcome.get());
		System.out.println("retry interruptible OK");
	}

	/** Each thread writes only its own variable: with per-variable locks these commits never conflict and run in parallel */
	static void disjointWritesRunInParallel() throws InterruptedException {
		final int threads = Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors()));
		final int perThread = 200_000;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final AtomicStamp<Integer>[] vars = new AtomicStamp[threads];
		for (int i = 0; i < threads; i++) vars[i] = new AtomicStamp<Integer>(0);
		long abortsBefore = Transaction.getAbortCount();
		long start = System.nanoTime();
		Thread[] ts = new Thread[threads];
		for (int t = 0; t < threads; t++) {
			final AtomicStamp<Integer> v = vars[t];
			ts[t] = new Thread(() -> {
				for (int i = 0; i < perThread; i++) {
					Transaction.atomic(() -> v.put(v.get() + 1));
				}
			});
			ts[t].start();
		}
		for (Thread t : ts) t.join();
		long ms = (System.nanoTime() - start) / 1_000_000;
		for (AtomicStamp<Integer> v : vars) check(v.get() == perThread, "v=" + v.get());
		System.out.println("disjoint writes OK: " + threads + " threads x " + perThread + " commits in " + ms
				+ "ms, conflict aborts: " + (Transaction.getAbortCount() - abortsBefore));
	}

	static void check(boolean ok, String msg) {
		if (!ok) {
			throw new AssertionError(msg);
		}
	}
}
