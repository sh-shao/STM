package com.dsc.test;

import com.dsc.Atomic.AtomicStamp;
import com.dsc.Transaction.Transaction;

/**
 * Multi-threaded counter plus bank transfers. Also checks that:
 * 1. read-modify-write never loses updates;
 * 2. the total balance stays constant during transfers;
 * 3. an application exception thrown from the body rolls the transaction back;
 * 4. nested (flattened) transactions work.
 */
public class Test04 {

	public static void main(String[] args) throws InterruptedException {
		counter();
		bank();
		rollbackOnException();
		nested();
		System.out.println("Test04 OK  commits=" + Transaction.getCommitCount()
				+ " aborts=" + Transaction.getAbortCount());
	}

	static void counter() throws InterruptedException {
		final AtomicStamp<Integer> counter = new AtomicStamp<Integer>(0);
		final int threads = 8, perThread = 20_000;
		runAll(threads, () -> {
			for (int i = 0; i < perThread; i++) {
				Transaction.atomic(() -> counter.put(counter.get() + 1));
			}
		});
		check(counter.get() == threads * perThread, "counter=" + counter.get());
		System.out.println("counter OK: " + counter.get());
	}

	static void bank() throws InterruptedException {
		final int n = 10, initial = 1000;
		@SuppressWarnings({ "unchecked", "rawtypes" })
		final AtomicStamp<Integer>[] accounts = new AtomicStamp[n];
		for (int i = 0; i < n; i++) {
			accounts[i] = new AtomicStamp<Integer>(initial);
		}
		final int expectedTotal = n * initial;
		final java.util.concurrent.atomic.AtomicBoolean done = new java.util.concurrent.atomic.AtomicBoolean();

		// Auditor thread: keeps checking the total in read-only transactions
		Thread auditor = new Thread(() -> {
			while (!done.get()) {
				int total = Transaction.atomic(() -> {
					int s = 0;
					for (AtomicStamp<Integer> a : accounts) s += a.get();
					return s;
				});
				check(total == expectedTotal, "auditor saw a wrong total: " + total);
			}
		});
		auditor.start();

		runAll(8, () -> {
			java.util.concurrent.ThreadLocalRandom r = java.util.concurrent.ThreadLocalRandom.current();
			for (int i = 0; i < 20_000; i++) {
				int from = r.nextInt(n), to = r.nextInt(n), amount = r.nextInt(50);
				Transaction.atomic(() -> {
					int f = accounts[from].get();
					if (f < amount) return; // insufficient funds: skip the transfer
					accounts[from].put(f - amount);
					accounts[to].put(accounts[to].get() + amount); // also correct when from == to (reads own write)
				});
			}
		});
		done.set(true);
		auditor.join();

		int total = 0;
		for (AtomicStamp<Integer> a : accounts) {
			check(a.get() >= 0, "negative balance");
			total += a.get();
		}
		check(total == expectedTotal, "total=" + total);
		System.out.println("bank OK: total=" + total);
	}

	static void rollbackOnException() {
		final AtomicStamp<String> s = new AtomicStamp<String>("old");
		try {
			Transaction.atomic(() -> {
				s.put("new");
				throw new IllegalStateException("business failure");
			});
		} catch (IllegalStateException expected) {
			// ok
		}
		check("old".equals(s.get()), "not rolled back after exception: " + s.get());
		System.out.println("rollback OK: " + s.get());
	}

	static void nested() {
		final AtomicStamp<Integer> x = new AtomicStamp<Integer>(1);
		int r = Transaction.atomic(() -> {
			x.put(10);
			int inner = Transaction.atomic(() -> x.get() + 1); // inner sees the outer's uncommitted write
			x.put(inner);
			return x.get();
		});
		check(r == 11 && x.get() == 11, "nested r=" + r + " x=" + x.get());
		System.out.println("nested OK: " + x.get());
	}

	static void runAll(int threads, Runnable task) throws InterruptedException {
		Thread[] ts = new Thread[threads];
		for (int i = 0; i < threads; i++) {
			ts[i] = new Thread(task);
			ts[i].start();
		}
		for (Thread t : ts) {
			t.join();
		}
	}

	static void check(boolean ok, String msg) {
		if (!ok) {
			throw new AssertionError(msg);
		}
	}
}
