package com.dsc.Atomic;

import java.util.concurrent.atomic.AtomicStampedReference;

public interface AtomicFactory {
	void setAtomicStamp(Object initialRef);
	 
	AtomicStampedReference getAtomicStamp();

}
