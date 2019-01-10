package com.aliyun.openservices.loghub.client;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts.CursorMode;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.BatchGetLogResponse;
import com.aliyun.openservices.loghub.client.exceptions.LogHubCheckPointException;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class LogHubAbstractClientAdaptor {
    protected Client mClient;
    protected ReadWriteLock mRWLock = new ReentrantReadWriteLock();
    protected final String mProject;
    protected final String mLogStoreExpr;
    protected final String mConsumerGroup;
    protected final String mConsumer;
    protected final String mUserAgent;
    protected final boolean mUseDirectMode;

    public LogHubAbstractClientAdaptor(String endPoint, String accessKeyId, String accessKey, String stsToken,
                                       String project, String logStoreExpr, String consumerGroup, String consumer, boolean useDirectMode) {
        this.mUseDirectMode = useDirectMode;
        this.mClient = new Client(endPoint, accessKeyId, accessKey);
        if (this.mUseDirectMode) {
            this.mClient.EnableDirectMode();
        }
        if (stsToken != null) {
            this.mClient.SetSecurityToken(stsToken);
        }
        this.mProject = project;
        this.mLogStoreExpr = logStoreExpr;
        this.mConsumerGroup = consumerGroup;
        this.mConsumer = consumer;
        this.mUserAgent = "consumergroup-java-" + consumerGroup;
        this.mClient.setUserAgent(mUserAgent);
    }

    public void SwitchClient(String endPoint, String accessKeyId, String accessKey, String stsToken) {
        mRWLock.writeLock().lock();
        this.mClient = new Client(endPoint, accessKeyId, accessKey);
        if (this.mUseDirectMode) {
            this.mClient.EnableDirectMode();
        }
        if (stsToken != null) {
            this.mClient.SetSecurityToken(stsToken);
        }
        mRWLock.writeLock().unlock();
    }

    public abstract void CreateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException;

    public abstract void UpdateConsumerGroup(final int timeoutInSec, final boolean inOrder) throws LogException;

    public abstract ConsumerGroup GetConsumerGroup() throws LogException;

    public abstract boolean HeartBeat(Map<String, ArrayList<Integer>> logStoreShards, Map<String, ArrayList<Integer>> response);

    public abstract void UpdateCheckPoint(final String logStore, final int shard, final String checkpoint) throws LogException;

    public abstract String GetCheckPoint(final String logStore, final int shard) throws LogException, LogHubCheckPointException;

    public abstract String GetCursor(final String logStore, final int shard, CursorMode mode) throws LogException;

    public abstract String GetCursor(final String logStore, final int shard, final long time) throws LogException;

    public abstract BatchGetLogResponse BatchGetLogs(final String logStore, final int shard, final int lines, final String cursor) throws LogException;
}
