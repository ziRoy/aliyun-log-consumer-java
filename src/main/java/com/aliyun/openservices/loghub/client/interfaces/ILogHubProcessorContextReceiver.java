package com.aliyun.openservices.loghub.client.interfaces;

import com.aliyun.openservices.loghub.client.LogHubProcessorContext;

public interface ILogHubProcessorContextReceiver {
    /**
     * Inject context information before the processor begins to consumer data
     * @param context the context about which shard will be consumed
     */
    void initialize(LogHubProcessorContext context);
}
