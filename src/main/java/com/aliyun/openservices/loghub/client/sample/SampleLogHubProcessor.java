package com.aliyun.openservices.loghub.client.sample;

import java.util.List;

import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.excpetions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.common.LogGroup;
import com.aliyun.openservices.loghub.common.LogItem;

public class SampleLogHubProcessor implements ILogHubProcessor {

	private String mShardId;
	private long mLastCheckTime = 0;
	@Override
	public void initialize(String shardId) {
		mShardId = shardId;
		
	}

	@Override
	public void process(List<LogGroup> logGroups,
			ILogHubCheckPointTracker checkPointTracker) {
		for (LogGroup group : logGroups) {
			List<LogItem> items = group.getAllLogs();
			
			for (LogItem item : items) {
				System.out.println("shard_id:" + mShardId + " " + item.toJSONString());
			}
		}
		long curTime = System.currentTimeMillis();
		if (curTime - mLastCheckTime > 1000 * 3) {
			try {
				checkPointTracker.saveCheckPoint(true);
			} catch (LogHubCheckPointException e) {
				
				e.printStackTrace();
			}
			mLastCheckTime = curTime;
		} else {
			try {
				checkPointTracker.saveCheckPoint(false);
			} catch (LogHubCheckPointException e) {
				
				e.printStackTrace();
			}
		}
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
		
		
	}
	

}