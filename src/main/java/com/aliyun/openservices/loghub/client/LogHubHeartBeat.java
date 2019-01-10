package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class LogHubHeartBeat {
	private ScheduledExecutorService threadpool;
	private LogHubAbstractClientAdaptor mLogHubClientAdapter;
	private boolean running = false;
	private final long mHeartBeatIntervalMillis;
	private Map<String, ArrayList<Integer>> mHeldShards;
	private Map<String, HashSet<Integer>> mHeartShards;
	private static final Logger logger = Logger.getLogger(LogHubHeartBeat.class);
	private static final long STOP_WAIT_TIME_MILLIS = 2000L;
	public LogHubHeartBeat(LogHubAbstractClientAdaptor logHubClientAdapter,
			long heartBeatIntervalMillis) {
		super();
		this.mLogHubClientAdapter = logHubClientAdapter;
		this.mHeartBeatIntervalMillis = heartBeatIntervalMillis;
		mHeldShards = new HashMap<String, ArrayList<Integer>>();
		mHeartShards = new HashMap<String, HashSet<Integer>>();
	}
	public void Start()
	{
		threadpool = Executors.newScheduledThreadPool(1, new LogThreadFactory());
		threadpool.scheduleWithFixedDelay(new HeartBeatRunnable(), 0L,
				mHeartBeatIntervalMillis, TimeUnit.MILLISECONDS);
		running = true;
	}
	/**
	 * Stops background threads.
	 */
	public void Stop() {
		if (threadpool != null) {
			threadpool.shutdown();
			try {
				if (threadpool.awaitTermination(STOP_WAIT_TIME_MILLIS,
						TimeUnit.MILLISECONDS)) {
				} else {
					threadpool.shutdownNow();

				}
			} catch (InterruptedException e) {

			}
		}
		running = false;
	}
	synchronized public void GetHeldShards(Map<String, ArrayList<Integer>> logStoreShards)
	{
		logStoreShards.clear();
		logStoreShards.putAll(mHeldShards);
	}
	synchronized public void RemoveHeartShard(final String logStore, final int shard)
	{
		HashSet<Integer> shards = mHeartShards.get(logStore);
		if (shards != null) {
			shards.remove(shard);
		}
	}
	synchronized protected void HeartBeat()
	{
		Map<String, ArrayList<Integer>> wrapper = new HashMap<String, ArrayList<Integer>>();
		for (Map.Entry<String, HashSet<Integer>> entry : mHeartShards.entrySet()) {
			wrapper.put(entry.getKey(), new ArrayList<Integer>(entry.getValue()));
		}
		if (mLogHubClientAdapter.HeartBeat(wrapper, mHeldShards)) {
			for (Map.Entry<String, ArrayList<Integer>> entry : mHeldShards.entrySet()) {
				HashSet<Integer> shardHash = mHeartShards.get(entry.getKey());
				if (shardHash == null) {
					mHeartShards.put(entry.getKey(), new HashSet<Integer>(entry.getValue()));
				} else {
					shardHash.addAll(entry.getValue());
				}
			}
		}
	}
	private class HeartBeatRunnable implements Runnable
	{
		@Override
		public void run() {
			try {
				HeartBeat();
			}
			catch (Throwable t) {
			}
		}	
	}
	
	/**
	 * @return true if this LeaseCoordinator is running
	 */
	public boolean isRunning() {
		return running;
	}
}
