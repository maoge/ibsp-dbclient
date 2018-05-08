package bench;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class LongIDGenerator {
	
	private static LongIDGenerator theInstance = null;
	private static ReentrantLock intanceLock = null;
	
	private AtomicLong id;
	
	static {
		intanceLock = new ReentrantLock();
	}
	
	public LongIDGenerator() {
		id = new AtomicLong(0);
	}
	
	public static LongIDGenerator get() {
		try {
			intanceLock.lock();
			if (theInstance != null){
				return theInstance;
			} else {
				theInstance = new LongIDGenerator();
			}
		} finally {
			intanceLock.unlock();
		}
		
		return theInstance;
	}
	
	public long nextID() {
		long value = id.incrementAndGet();
		if (value == Long.MAX_VALUE) {
			id.set(0);
		}
		
		return value;
	}

}
