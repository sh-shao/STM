package com.dsc.Transaction;

/**
 * Control-flow signal thrown by {@link Transaction#retry()}. It is caught first by the nearest
 * {@link Transaction#orElse}; without one, {@link Transaction#atomic} catches it and blocks.
 * Application code should not catch it.
 */
final class RetrySignal extends Error {
	private static final long serialVersionUID = 1L;

	static final RetrySignal INSTANCE = new RetrySignal();

	private RetrySignal() {
		super("transaction retry", null, false, false);
	}
}
