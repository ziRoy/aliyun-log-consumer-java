package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;

public class LogHubClientAdapter extends LogHubAbstractClientAdaptor {
    private static final Logger logger = Logger.getLogger(LogHubClientAdapter.class);

    public LogHubClientAdapter(String endPoint, String accessKeyId, String accessKey, String stsToken, String project, String logStoreExpr, String consumerGroup, String consumer, boolean useDirectMode) {
        super(endPoint, accessKeyId, accessKey, stsToken, project, logStoreExpr, consumerGroup, consumer, useDirectMode);
    }

    @Override
    public void CreateConsumerGroup(int timeoutInSec, boolean inOrder) throws LogException {
        mRWLock.readLock().lock();
        try {
            mClient.CreateConsumerGroup(mProject, mLogStoreExpr, new ConsumerGroup(mConsumerGroup, timeoutInSec, inOrder));
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public void UpdateConsumerGroup(int timeoutInSec, boolean inOrder) throws LogException {
        mRWLock.readLock().lock();
        try {
            mClient.UpdateConsumerGroup(mProject, mLogStoreExpr, mConsumerGroup, inOrder, timeoutInSec);
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public ConsumerGroup GetConsumerGroup() throws LogException {
        mRWLock.readLock().lock();
        try {
            for (ConsumerGroup consumerGroup : mClient.ListConsumerGroup(mProject, mLogStoreExpr).GetConsumerGroups()) {
                if (consumerGroup.getConsumerGroupName().compareTo(mConsumerGroup) == 0) {
                    return consumerGroup;
                }
            }
        } finally {
            mRWLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public boolean HeartBeat(Map<String, ArrayList<Integer>> logStoreShards, Map<String, ArrayList<Integer>> response) {
        ArrayList<Integer> shards = logStoreShards.get(mLogStoreExpr);
        if (shards == null) {
            return false;
        }
        mRWLock.readLock().lock();
        response.clear();
        try {
            response.put(mLogStoreExpr, mClient.HeartBeat(mProject, mLogStoreExpr, mConsumerGroup, mConsumer, shards).GetShards());
            return true;
        } catch (LogException e) {
            logger.warn(e.GetErrorCode() + ": " + e.GetErrorMessage());
        } finally {
            mRWLock.readLock().unlock();
        }
        return false;
    }

    @Override
    public void UpdateCheckPoint(String logStore, int shard, String checkpoint) throws LogException {
        mRWLock.readLock().lock();
        try {
            mClient.UpdateCheckPoint(mProject, logStore, mConsumerGroup, mConsumer, shard, checkpoint);
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public String GetCheckPoint(String logStore, int shard) throws LogException, LogHubCheckPointException {
        mRWLock.readLock().lock();
        ArrayList<ConsumerGroupShardCheckPoint> checkPoints;
        try {
            checkPoints = mClient.GetCheckPoint(mProject, logStore, mConsumerGroup, shard).GetCheckPoints();
        } finally {
            mRWLock.readLock().unlock();
        }
        if (checkPoints == null || checkPoints.size() == 0) {
            throw new LogHubCheckPointException("fail to get shard checkpoint");
        } else {
            return checkPoints.get(0).getCheckPoint();
        }
    }

    @Override
    public String GetCursor(String logStore, int shard, CursorMode mode) throws LogException {
        mRWLock.readLock().lock();
        try {
            return mClient.GetCursor(mProject, logStore, shard, mode).GetCursor();
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public String GetCursor(String logStore, int shard, long time) throws LogException {
        mRWLock.readLock().lock();
        try {
            return mClient.GetCursor(mProject, logStore, shard, time).GetCursor();
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public BatchGetLogResponse BatchGetLogs(String logStore, int shard, int lines, String cursor) throws LogException {
        mRWLock.readLock().lock();
        try {
            BatchGetLogResponse response = mClient.BatchGetLog(mProject, logStore, shard, lines, cursor);
            return response;
        } finally {
            mRWLock.readLock().unlock();
        }
    }
}