package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.ConsumerGroupShardCheckPoint;
import com.aliyun.openservices.log.common.ProjectConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Map;

public class LogHubProjectClientAdaptor extends LogHubAbstractClientAdaptor {
    private static final Logger logger = Logger.getLogger(LogHubProjectClientAdaptor.class);

    public LogHubProjectClientAdaptor(String endPoint, String accessKeyId, String accessKey, String stsToken, String project, String logStoreExpr, String consumerGroup, String consumer, boolean useDirectMode) {
        super(endPoint, accessKeyId, accessKey, stsToken, project, logStoreExpr, consumerGroup, consumer, useDirectMode);
    }

    @Override
    public void CreateConsumerGroup(int timeoutInSec, boolean inOrder) throws LogException {
        mRWLock.readLock().lock();
        try {
            mClient.CreateProjectConsumerGroup(mProject, new ProjectConsumerGroup(mConsumerGroup, mLogStoreExpr, timeoutInSec, inOrder));
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public void UpdateConsumerGroup(int timeoutInSec, boolean inOrder) throws LogException {
        mRWLock.readLock().lock();
        try {
            mClient.UpdateProjectConsumerGroup(mProject, mConsumerGroup, inOrder, timeoutInSec);
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public ConsumerGroup GetConsumerGroup() throws LogException {
        mRWLock.readLock().lock();
        try {
            for (ProjectConsumerGroup consumerGroup : mClient.ListProjectConsumerGroup(mProject).getConsumerGroups()) {
                if (consumerGroup.getConsumerGroupName().compareTo(mConsumerGroup) == 0) {
                    return new ConsumerGroup(consumerGroup.getConsumerGroupName(), consumerGroup.getTimeout(), consumerGroup.isInOrder());
                }
            }
        } finally {
            mRWLock.readLock().unlock();
        }
        return null;
    }

    @Override
    public boolean HeartBeat(Map<String, ArrayList<Integer>> logStoreShards, Map<String, ArrayList<Integer>> response) {
        mRWLock.readLock().lock();
        response.clear();
        try {
            response.putAll(
                    mClient.ProjectConsumerGroupHeartBeat(mProject, mConsumerGroup, mConsumer, logStoreShards).getLogStoreShards()
            );
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
            mClient.UpdateProjectConsumerGroupCheckPoint(mProject, mConsumerGroup, mConsumer, logStore, shard, checkpoint);
        } finally {
            mRWLock.readLock().unlock();
        }
    }

    @Override
    public String GetCheckPoint(String logStore, int shard) throws LogException, LogHubCheckPointException {
        mRWLock.readLock().lock();
        Map<String, ArrayList<ConsumerGroupShardCheckPoint>> checkPoints = null;
        try {
            checkPoints = mClient.GetProjectConsumerGroupCheckPoint(mProject, mConsumerGroup, logStore, shard).getCheckPoints();
        } finally {
            mRWLock.readLock().unlock();
        }
        String cp = null;
        if (checkPoints != null) {
            ArrayList<ConsumerGroupShardCheckPoint> list = checkPoints.get(logStore);
            if (list != null && list.size() > 0) {
                cp = list.get(0).getCheckPoint();
            }
        }
        if (cp == null) {
            throw new LogHubCheckPointException("fail to get shard checkpoint");
        }
        return cp;
    }

    @Override
    public String GetCursor(String logStore, int shard, Consts.CursorMode mode) throws LogException {
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
