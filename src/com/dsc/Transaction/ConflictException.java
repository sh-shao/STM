package com.dsc.Transaction;

/**
 * Transaction conflict signal: the transaction read data newer than its snapshot, hit a variable
 * that another transaction is committing, or failed validation at commit time.
 * {@link Transaction#atomic} catches it and retries automatically.
 * <p>
 * It extends Error rather than RuntimeException so that a {@code catch (Exception e)} in application
 * code does not swallow it by accident. Even if it is swallowed, the transaction is already marked as
 * doomed, so any further operation or the commit throws it again.
 */
public final class ConflictException extends Error {
	private static final long serialVersionUID = 1L;

	static final ConflictException INSTANCE = new ConflictException();

	private ConflictException() {
		// Used for control flow only: no stack trace, to keep frequent aborts cheap
		super("transaction conflict", null, false, false);
	}
}
