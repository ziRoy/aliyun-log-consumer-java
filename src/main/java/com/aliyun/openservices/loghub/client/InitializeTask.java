package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.loghub.client.config.LogHubCursorPosition;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessorContextReceiver;

public class InitializeTask implements ITask {

	private LogHubAbstractClientAdaptor mLogHubClientAdapter;
	private ILogHubProcessor mProcessor;
	private String mLogStore;
	private int mShardId;
	private LogHubCursorPosition mCursorPosition;
	private long mCursorStartTime;

	public InitializeTask(ILogHubProcessor processor, LogHubAbstractClientAdaptor logHubClientAdapter,
			String logStore, int shardId,
			LogHubCursorPosition cursorPosition, long cursorStartTime) {
		mProcessor = processor;
		mLogHubClientAdapter = logHubClientAdapter;
		mLogStore = logStore;
		mShardId = shardId;
		mCursorPosition = cursorPosition;
		mCursorStartTime = cursorStartTime;
	}

	public TaskResult call() {
		try {
			if (mProcessor instanceof ILogHubProcessorContextReceiver) {
				((ILogHubProcessorContextReceiver) mProcessor).initialize(new LogHubProcessorContext(mLogStore, mShardId));
			}
			mProcessor.initialize(mShardId);
			boolean is_cursor_persistent = false;
			String checkPoint = mLogHubClientAdapter.GetCheckPoint(mLogStore, mShardId);
			String cursor = null;
			if (checkPoint != null && checkPoint.length() > 0) {
				is_cursor_persistent = true;
				cursor = checkPoint;
			} else {
				// get cursor from loghub client , begin or end
				if(mCursorPosition.equals(LogHubCursorPosition.BEGIN_CURSOR))
				{
					cursor = mLogHubClientAdapter.GetCursor(mLogStore, mShardId, CursorMode.BEGIN);
				}
				else if (mCursorPosition.equals(LogHubCursorPosition.END_CURSOR))
				{
					cursor = mLogHubClientAdapter.GetCursor(mLogStore, mShardId, CursorMode.END);
				}
				else
				{
					cursor = mLogHubClientAdapter.GetCursor(mLogStore, mShardId, mCursorStartTime);
				}
			}
			return new InitTaskResult(cursor, is_cursor_persistent);
		} catch (Exception e) {
			return new TaskResult(e);
		}
	}
}
