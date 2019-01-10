package com.aliyun.openservices.loghub.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javafx.util.Pair;
import org.apache.log4j.Logger;

import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.aliyun.openservices.loghub.client.exceptions.LogHubClientWorkerException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorFactory;


public class ClientWorker implements Runnable
{
	// typedef
	private class LogStoreShard extends Pair<String, Integer> {
		public LogStoreShard(String key, Integer value) {
			super(key, value);
		}
	}
	private final ILogHubProcessorFactory mLogHubProcessorFactory;
	private final LogHubConfig mLogHubConfig;
	private final LogHubHeartBeat mLogHubHeartBeat;
	private boolean mShutDown = false;
	private final Map<LogStoreShard, LogHubConsumer> mShardConsumer = new HashMap<LogStoreShard, LogHubConsumer>();
	private final ExecutorService mExecutorService = Executors.newCachedThreadPool(new LogThreadFactory());
	private LogHubAbstractClientAdaptor mLogHubClientAdapter;
	private static final Logger logger = Logger.getLogger(ClientWorker.class);
	private boolean mMainLoopExit = false;

	public ClientWorker(ILogHubProcessorFactory factory, LogHubConfig config) throws LogHubClientWorkerException {
		mLogHubProcessorFactory = factory;
		mLogHubConfig = config;
		if (mLogHubConfig.isProjectConsumerGroup()) {
			mLogHubClientAdapter = new LogHubProjectClientAdaptor(
					config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
					config.getLogStore(), config.getConsumerGroupName(), config.getConsumerName(), config.isDirectModeEnabled());
		} else {
			mLogHubClientAdapter = new LogHubClientAdapter(
					config.getLogHubEndPoint(), config.getAccessId(), config.getAccessKey(), config.getStsToken(), config.getProject(),
					config.getLogStore(), config.getConsumerGroupName(), config.getConsumerName(), config.isDirectModeEnabled());
		}
		try
		{
			mLogHubClientAdapter.CreateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
		} 
		catch (LogException e) 
		{
			if(e.GetErrorCode().compareToIgnoreCase("ConsumerGroupAlreadyExist") == 0)
			{
				try {
					mLogHubClientAdapter.UpdateConsumerGroup((int)(config.getHeartBeatIntervalMillis()*2/1000), config.isConsumeInOrder());
				} catch (LogException e1) {
					throw new LogHubClientWorkerException("error occour when update consumer group, errorCode: " + e1.GetErrorCode() + ", errorMessage: " + e1.GetErrorMessage());
				}
			}
			else
			{
				throw new LogHubClientWorkerException("error occour when create consumer group, errorCode: " + e.GetErrorCode() + ", errorMessage: " + e.GetErrorMessage());
			}
		}
		mLogHubHeartBeat = new LogHubHeartBeat(mLogHubClientAdapter, config.getHeartBeatIntervalMillis());
	}
	public void SwitchClient(String accessKeyId, String accessKey)
	{
		mLogHubClientAdapter.SwitchClient(mLogHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, null);
	}
	public void SwitchClient(String accessKeyId, String accessKey, String stsToken)
	{
		mLogHubClientAdapter.SwitchClient(mLogHubConfig.getLogHubEndPoint(), accessKeyId, accessKey, stsToken);
	}
	public void run() {
		mLogHubHeartBeat.Start();
		Map<String, ArrayList<Integer>> heldShards = new HashMap<String, ArrayList<Integer>>();
		while (mShutDown == false) {
			mLogHubHeartBeat.GetHeldShards(heldShards);
			for (Map.Entry<String, ArrayList<Integer>> entry : heldShards.entrySet()) {
				for (int shard : entry.getValue()) {
					LogHubConsumer consumer = getConsumer(entry.getKey(), shard);
					consumer.consume();
				}
			}
			cleanConsumer(heldShards);
			try {
				Thread.sleep(mLogHubConfig.getDataFetchIntervalMillis());
			} catch (InterruptedException e) {

			}
		}
		mMainLoopExit = true;
	}
	public void shutdown()
	{
		this.mShutDown = true;
		int times = 0 ;
		while(!mMainLoopExit && times++ < 20) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
		for(LogHubConsumer consumer: mShardConsumer.values()){
			consumer.shutdown();
		}
		mExecutorService.shutdown();
		try {
			mExecutorService.awaitTermination(30, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		mLogHubHeartBeat.Stop();
	}
	
	private void cleanConsumer(Map<String, ArrayList<Integer>> ownedShards)
	{
		for (Iterator<Entry<LogStoreShard, LogHubConsumer>> it = mShardConsumer.entrySet().iterator(); it.hasNext(); ) {
			Entry<LogStoreShard, LogHubConsumer> entry = it.next();
			LogHubConsumer consumer = entry.getValue();
			String logStore = entry.getKey().getKey();
			Integer shard = entry.getKey().getValue();

			ArrayList<Integer> shards = ownedShards.get(logStore);
			if (shards == null || !shards.contains(shard)) {
				consumer.shutdown();
				logger.info("try to shut down a consumer shard:" + logStore + "$" + shard);
			}
			if (consumer.isShutdown()) {
				mLogHubHeartBeat.RemoveHeartShard(logStore, shard);
				it.remove();
				logger.info("remove a consumer shard:" + logStore + "$" + shard);
			}
		}
	}
	
	private LogHubConsumer getConsumer(final String logStore, final int shardId)
	{
	    LogStoreShard logStoreShard = new LogStoreShard(logStore, shardId);
		LogHubConsumer consumer = mShardConsumer.get(logStoreShard);
		if (consumer != null)
		{
			return consumer;
		}
		consumer = new LogHubConsumer(mLogHubClientAdapter, logStore, shardId,
				mLogHubConfig.getConsumerName(),
				mLogHubProcessorFactory.generatorProcessor(), mExecutorService,
				mLogHubConfig.getCursorPosition(),
				mLogHubConfig.GetCursorStartTime(),
				mLogHubConfig.getMaxFetchLogGroupSize());
		mShardConsumer.put(logStoreShard, consumer);
		logger.info("create a consumer shard:" + logStore + "$" + shardId);
		return consumer;
	}
}
