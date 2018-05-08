package test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Statistic {
	private String flag;
	private AtomicLong maxTPS;
	private AtomicLong[] normalCntVec;
	private AtomicLong[] errorCntVec;

	private volatile long lastNormalTotalCnt;
	private volatile long lastErrorlTotalCnt;
	private volatile long lastTPS;
	private volatile long avgTPS;

	private ScheduledExecutorService statRunnerExec;
	private Runnable statRunner;

	private ScheduledExecutorService statPrintExec;
	private Runnable statPrinter;

	private static final long STAT_INTERVAL = 1000L;
	private static final long PRINT_INTERVAL = 5000L;

	private volatile long begTS;
	private volatile long lastTS;

	public Statistic(String flag, AtomicLong maxTPS, AtomicLong[] normalCntVec, AtomicLong[] errorCntVec) {
		this.flag = flag;
		this.maxTPS = maxTPS;
		this.normalCntVec = normalCntVec;
		this.errorCntVec = errorCntVec;
		this.lastNormalTotalCnt = 0;
		this.lastErrorlTotalCnt = 0;

		this.begTS = System.currentTimeMillis();
		this.lastTS = begTS;

		statRunner = new StatRunner(this);
		statRunnerExec = Executors.newSingleThreadScheduledExecutor();
		statRunnerExec.scheduleAtFixedRate(statRunner, STAT_INTERVAL, STAT_INTERVAL, TimeUnit.MILLISECONDS);

		statPrinter = new StatPrinter(this);
		statPrintExec = Executors.newSingleThreadScheduledExecutor();
		statPrintExec.scheduleAtFixedRate(statPrinter, PRINT_INTERVAL, PRINT_INTERVAL, TimeUnit.MILLISECONDS);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				statRunnerExec.shutdown();
				statPrintExec.shutdown();
			}
		});
	}

	public void StopRunning() {
		if (!statRunnerExec.isShutdown()) {
			statRunnerExec.shutdown();
		}

		if (!statPrintExec.isShutdown()) {
			statPrintExec.shutdown();
		}
		
		computeStatInfo();
		printStatInfo();
	}

	private long getCurrNormalTotal() {
		long currTotalCnt = 0;

		for (AtomicLong ai : normalCntVec) {
			currTotalCnt += ai.get();
		}

		return currTotalCnt;
	}
	
	private long getCurrErrorTotal() {
		long currTotalCnt = 0;

		for (AtomicLong ai : errorCntVec) {
			currTotalCnt += ai.get();
		}

		return currTotalCnt;
	}

	public void computeStatInfo() {
		long currTotalCnt = getCurrNormalTotal();
		long currTS = System.currentTimeMillis();

		long diff = currTotalCnt - lastNormalTotalCnt;

		if (currTS > lastTS) {
			lastTPS = diff * 1000 / (currTS - lastTS);
			avgTPS = currTotalCnt * 1000 / (currTS - begTS);

			if (lastTPS > maxTPS.get()) {
				maxTPS.set(lastTPS);
			}

			lastTS = currTS;
			lastNormalTotalCnt = currTotalCnt;
		}
		
		lastErrorlTotalCnt = getCurrErrorTotal();
	}

	public void printStatInfo() {
		String print = String.format("%s Statistic runs for:%d seconds, total normal processed:%d, Last TPS:%d, Avg TPS:%d, Max TPS:%d, total error:%d",
				flag, (lastTS - begTS) / 1000, lastNormalTotalCnt, lastTPS, avgTPS, maxTPS.get(), lastErrorlTotalCnt);

		System.out.println("--------------------------------------------------------------------------------------------------");
		System.out.println(print);
		System.out.println("--------------------------------------------------------------------------------------------------");
	}

	private static class StatRunner implements Runnable {

		private Statistic statistic;

		public StatRunner(Statistic statistic) {
			this.statistic = statistic;
		}

		@Override
		public void run() {
			statistic.computeStatInfo();
		}

	}

	private static class StatPrinter implements Runnable {

		private Statistic statistic;

		public StatPrinter(Statistic statistic) {
			this.statistic = statistic;
		}

		@Override
		public void run() {
			statistic.printStatInfo();
		}

	}

}
