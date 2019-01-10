import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.LogHubProcessorContext;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorContextReceiver;


public class LogHubTestProcessor implements ILogHubProcessor, ILogHubProcessorContextReceiver {
	private long mLastCheckTime = 0;
	private String mLogStore;
	private int mShard;
	@Override
	public void initialize(int shardId) {
	}

	@Override
	public void initialize(LogHubProcessorContext context) {
		mLogStore = context.getLogStoreName();
		mShard = context.getShardId();
		System.out.println("initialize: " + mLogStore + "," + mShard);
	}

	@Override
	public String process(List<LogGroupData> logGroups,
			ILogHubCheckPointTracker checkPointTracker) 
	{
		long curTime = System.currentTimeMillis();
	    if (curTime - mLastCheckTime >  60 * 1000) {
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
		return null;
	}

	@Override
	public void shutdown(ILogHubCheckPointTracker checkPointTracker) 
	{
	}

}
