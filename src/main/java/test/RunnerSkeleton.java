package test;

public abstract class RunnerSkeleton {
	
	protected volatile boolean bRunning = true;
	
	public boolean isRunning() {
		return bRunning;
	}
	
	public void stopRunning() {
		bRunning = false;
	}

}
