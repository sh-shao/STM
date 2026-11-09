package com.dsc.Atomic;

import com.dsc.Transaction.TVar;
import com.dsc.Transaction.Transaction;

/**
 * A transactional variable. The value and its version (stamp) are always read and written
 * together as one atomic unit.
 * <p>
 * Inside {@link Transaction#atomic}, {@link #get()} / {@link #put(Object)} go through the current
 * transaction (read set / write set) and only take effect on commit.
 * Outside a transaction, get is a plain atomic read and put is automatically wrapped in a
 * single-write transaction.
 */
public class AtomicStamp<T> extends TVar<T> {

	public AtomicStamp(T initialValue) {
		super(initialValue);
	}

	/** Currently committed version (same as {@link #getVersion()}), for debugging/observation only. */
	public long getStamp() {
		return getVersion();
	}
}
