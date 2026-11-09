package com.dsc.Transaction;

import java.util.concurrent.atomic.AtomicReference;





public class ActiveTransactionsRecord {
	  
	public final int transactionNumber;
	private final AtomicReference<ActiveTransactionsRecord> next =new AtomicReference<ActiveTransactionsRecord>(null);
	public ActiveTransactionsRecord(int transactionNumber){
		this.transactionNumber=transactionNumber;
		
	}
	
	
	// commit status
	private volatile boolean recordCommitted = false;
	public ActiveTransactionsRecord getNext() {
	        return next.get();
	}

	
	 public boolean trySetNext(ActiveTransactionsRecord next) {
	        return this.next.compareAndSet(null, next);
	 }

	 protected void setCommitted() {
	        this.recordCommitted = true;
	 }

	 public boolean isCommitted() {
	        return this.recordCommitted;
	 }
}
