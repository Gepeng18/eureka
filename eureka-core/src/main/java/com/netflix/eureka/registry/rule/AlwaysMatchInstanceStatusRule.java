package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This rule matches always and returns the current status of the instance.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class AlwaysMatchInstanceStatusRule implements InstanceStatusOverrideRule {
    private static final Logger logger = LoggerFactory.getLogger(AlwaysMatchInstanceStatusRule.class);

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo,
                                      Lease<InstanceInfo> existingLease,
                                      boolean isReplication) {
        logger.debug("Returning the default instance status {} for instance {}", instanceInfo.getStatus(),
                instanceInfo.getId());
        // 直接使用外来的status，为什么这样呢？因为服务端已经是known了，且client传来的是up或者outOfService（第一个rule过滤了其他情况），
        // 这时候服务端认了，返回结果不是up就是outOfService
        return StatusOverrideResult.matchingStatus(instanceInfo.getStatus());
    }

    @Override
    public String toString() {
        return AlwaysMatchInstanceStatusRule.class.getName();
    }
}
