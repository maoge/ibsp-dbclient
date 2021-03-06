package bench;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import ibsp.common.utils.PropertiesUtils;
import ibsp.dbclient.DbSource;
import ibsp.dbclient.exception.DBException;
import ibsp.dbclient.pool.ConnectionPool;

public class BenchSkeleton {
	
	private static final String TEST_PROP_FILE = "test";
	
	private static AtomicLong[] normalCntVec;
	private static AtomicLong[] errorCntVec;
	private static AtomicLong maxTPS;
	
	private static void startTest(PropertiesUtils prop)
			throws InstantiationException, IllegalAccessException, InterruptedException, NoSuchMethodException,
			SecurityException, IllegalArgumentException, InvocationTargetException {
		
		String flag        = prop.get("flag");
		String workerClazz = prop.get("worker.class");
		int    totalTime   = prop.getInt("totalTime");
		int    paralle     = prop.getInt("paralle");
		
		Class<?> clazz = null;
		try {
			clazz = Class.forName(workerClazz);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		
		if (paralle <= 0) {
			String info = String.format("parallel param:%d illegal", paralle);
			System.out.println(info);
			return;
		}
		
		if (totalTime <= 0) {
			String info = String.format("totalTime param:%d illegal", totalTime);
			System.out.println(info);
			return;
		}
		
		try {
			DbSource.get().getPool();
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		normalCntVec = new AtomicLong[paralle];
		errorCntVec = new AtomicLong[paralle];
		for (int i = 0; i < paralle; i++) {
			normalCntVec[i] = new AtomicLong(0);
			errorCntVec[i] = new AtomicLong(0);
		}
		maxTPS = new AtomicLong(0);
		
		Statistic stat = new Statistic(flag, maxTPS, normalCntVec, errorCntVec);
		
		long start = System.currentTimeMillis();
		Vector<RunnerSkeleton> workerThreads = new Vector<RunnerSkeleton>(paralle);
		
		// start bench worker
		for (int idx = 0; idx < paralle; idx++) {
			String name = String.format("bench_worker_%d", idx);
			Constructor<?> constructor = clazz.getConstructor(AtomicLong.class, AtomicLong.class, PropertiesUtils.class);
			Object runner = constructor.newInstance(normalCntVec[idx], errorCntVec[idx], prop);
			
			Thread t = new Thread((Runnable)runner);
			t.setDaemon(true);
			t.setName(name);
			t.start();
			
			workerThreads.add((RunnerSkeleton) runner);
		}
		
		
		long curr = System.currentTimeMillis();
		while ((curr - start) < totalTime) {
			Thread.sleep(1000);
		}
		
		
		for (Runnable r : workerThreads) {
			RunnerSkeleton runner = (RunnerSkeleton) r;
			runner.stopRunning();
		}
		
		stat.StopRunning();
		
		try {
			ConnectionPool pool = DbSource.get().getPool();
			pool.close();
		} catch (DBException e) {
			e.printStackTrace();
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		PropertiesUtils prop = null;
		try {
			prop = PropertiesUtils.getInstance(TEST_PROP_FILE);
		} catch (Exception e) {
			String err = String.format("load property file:%s error!", TEST_PROP_FILE);
			System.out.println(err);
			return;
		}
		
		try {
			startTest(prop);
		} catch (Exception e) {
			;
		}

	}

}
