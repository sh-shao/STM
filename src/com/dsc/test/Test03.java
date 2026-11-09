package com.dsc.test;

import com.dsc.Atomic.AtomicStamp;
import com.dsc.Transaction.Transaction;

/**
 * Two variables must change together: writer threads increment both in the same transaction,
 * and an observer reads both in a read-only transaction. It must always see num1 == num2.
 */
public class Test03 {

	static final AtomicStamp<Integer> num1 = new AtomicStamp<Integer>(0);
	static final AtomicStamp<Integer> num2 = new AtomicStamp<Integer>(0);

	static final int WRITERS = 4;
	static final int INCREMENTS = 50_000;

	public static void main(String[] args) throws InterruptedException {
		Thread[] writers = new Thread[WRITERS];
		for (int t = 0; t < WRITERS; t++) {
			writers[t] = new Thread(() -> {
				for (int i = 0; i < INCREMENTS; i++) {
					Transaction.atomic(() -> {
						num1.put(num1.get() + 1);
						num2.put(num2.get() + 1);
					});
				}
			});
			writers[t].start();
		}

		long checks = 0;
		boolean running = true;
		while (running) {
			running = false;
			for (Thread w : writers) {
				running |= w.isAlive();
			}
			int[] snap = Transaction.atomic(() -> new int[] { num1.get(), num2.get() });
			if (snap[0] != snap[1]) {
				throw new AssertionError("inconsistent: num1=" + snap[0] + " num2=" + snap[1]);
			}
			checks++;
		}
		for (Thread w : writers) {
			w.join();
		}

		int expected = WRITERS * INCREMENTS;
		System.out.println("num1=" + num1.get() + " num2=" + num2.get() + " expected=" + expected
				+ " consistency checks=" + checks
				+ " commits=" + Transaction.getCommitCount() + " aborts=" + Transaction.getAbortCount());
		if (num1.get() != expected || num2.get() != expected) {
			throw new AssertionError("wrong result");
		}
		System.out.println("Test03 OK");
	}
}
