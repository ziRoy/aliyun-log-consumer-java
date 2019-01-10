package com.aliyun.openservices.loghub.client;

public class LogHubProcessorContext {
    private String logStoreName;
    private int shardId;

    public LogHubProcessorContext(String logStoreName, int shardId) {
        this.logStoreName = logStoreName;
        this.shardId = shardId;
    }

    public String getLogStoreName() {
        return logStoreName;
    }

    public void setLogStoreName(String logStoreName) {
        this.logStoreName = logStoreName;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }
}
