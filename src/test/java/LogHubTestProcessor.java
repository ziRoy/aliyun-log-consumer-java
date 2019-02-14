import java.util.List;

import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Logs;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.LogHubProcessorContext;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorContextReceiver;


public class LogHubTestProcessor implements ILogHubProcessor, ILogHubProcessorContextReceiver {
	private long mLastCheckTime = 0;
	private String mLogStore;
	private int mShard;
	private long mSeq = 0;
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
	    if (logGroups != null) {
			for (LogGroupData data : logGroups) {
				try {
					Logs.LogGroup group = data.GetLogGroup();
					if (group != null && group.getLogsList() != null) {
						List<Logs.Log> logs = group.getLogsList();
						for (Logs.Log log : logs) {
							StringBuilder sb = new StringBuilder(mLogStore + "." + mShard + ": ");
							for (Logs.Log.Content content : log.getContentsList()) {
								String key = content.getKey();
								String value = content.getValue();
								sb.append("<").append(key).append(":").append(value).append(">");
								if (key.equalsIgnoreCase("content"))
								{
									long seq = Long.parseLong(value);
									if (mSeq >= seq) {
										System.err.println("logstore: " + mLogStore + " shard: " + mShard + " get wrong seq prev: " + mSeq + " next: " + seq);
									}
									mSeq = seq;
								}
							}
							System.out.println(sb.toString());
						}
					}
				} catch (LogException e) {
					e.printStackTrace();
				}
			}
		}
		long curTime = System.currentTimeMillis();
	    if (curTime - mLastCheckTime >  10 * 1000) {
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
	    System.out.println("stop consume: " + mLogStore + " " + mShard);
	}

}
