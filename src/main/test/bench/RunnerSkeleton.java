package bench;

import java.util.concurrent.atomic.AtomicLong;

public abstract class RunnerSkeleton implements Runnable {
	
	protected volatile boolean bRunning = true;
	protected AtomicLong normalCnt;
	protected AtomicLong errorCnt;
	
	public RunnerSkeleton(AtomicLong normalCnt, AtomicLong errorCnt) {
		this.normalCnt = normalCnt;
		this.errorCnt = errorCnt;
	}
	
	public boolean isRunning() {
		return bRunning;
	}
	
	public void stopRunning() {
		bRunning = false;
	}
	
	@Override
	public void run() {
		while (isRunning()) {
			if (doWork()) {
				//normalCnt.incrementAndGet();
				normalCnt.addAndGet(100);
			} else {
				errorCnt.incrementAndGet();
			}
		}
	}
	
	public abstract boolean doWork();

}
