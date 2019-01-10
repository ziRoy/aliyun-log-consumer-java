package com.aliyun.openservices.loghub.client;

import java.util.List;

import org.apache.log4j.Logger;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;

public class LogHubFetchTask implements ITask {
	private LogHubAbstractClientAdaptor mLogHubClientAdapter;
	private String mLogStore;
	private int mShardId;
	private String mCursor;
	private int mMaxFetchLogGroupSize;
	private static final Logger logger = Logger.getLogger(LogHubFetchTask.class);

	public LogHubFetchTask(LogHubAbstractClientAdaptor logHubClientAdapter, String logStore, int shardId, String cursor, int maxFetchLogGroupSize) {
		mLogHubClientAdapter = logHubClientAdapter;
		mLogStore = logStore;
		mShardId = shardId;
		mCursor = cursor;
		mMaxFetchLogGroupSize = maxFetchLogGroupSize;
	}

	public TaskResult call() {
		Exception exception = null;
		for (int retry = 0 ; retry < 2 ; retry++)
		{
			try {
				BatchGetLogResponse response = mLogHubClientAdapter.BatchGetLogs(
						mLogStore, mShardId, mMaxFetchLogGroupSize, mCursor);
				List<LogGroupData> fetchedData = response.GetLogGroups();
				logger.debug("shard id = " + mShardId + " cursor = " + mCursor
						+ " next cursor" + response.GetNextCursor() + " size:"
						+ String.valueOf(response.GetCount()));
				
				String cursor = response.GetNextCursor();
				
				if (cursor.isEmpty()) {
					return new FetchTaskResult(fetchedData, mCursor, response.GetRawSize());
				} else {
					return new FetchTaskResult(fetchedData, cursor, response.GetRawSize());
				}
			} catch (Exception e) {
				exception = e;
			}
		
			// only retry if the first request get "SLSInvalidCursor" exception
			if (retry == 0
					&& exception instanceof LogException
					&& ((LogException) (exception)).GetErrorCode()
							.toLowerCase().indexOf("invalidcursor") != -1) {
				try {
					freshCursor();
				} catch (Exception e) {
					return new TaskResult(exception);
				}
				continue;
			} else {
				break;
			}
		}
		return new TaskResult(exception);
	}

	public void freshCursor() throws NumberFormatException, LogException {
		mCursor =  mLogHubClientAdapter.GetCursor(mLogStore, mShardId, CursorMode.END);
	}
}
