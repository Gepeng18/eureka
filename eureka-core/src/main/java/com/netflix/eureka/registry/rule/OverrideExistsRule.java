package com.netflix.eureka.registry.rule;

import com.netflix.appinfo.InstanceInfo;
import com.netflix.eureka.lease.Lease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * This rule checks to see if we have overrides for an instance and if we do then we return those.
 *
 * Created by Nikos Michalakis on 7/13/16.
 */
public class OverrideExistsRule implements InstanceStatusOverrideRule {

    private static final Logger logger = LoggerFactory.getLogger(OverrideExistsRule.class);

    private Map<String, InstanceInfo.InstanceStatus> statusOverrides;

    public OverrideExistsRule(Map<String, InstanceInfo.InstanceStatus> statusOverrides) {
        this.statusOverrides = statusOverrides;
    }

    @Override
    public StatusOverrideResult apply(InstanceInfo instanceInfo, Lease<InstanceInfo> existingLease, boolean isReplication) {
        InstanceInfo.InstanceStatus overridden = statusOverrides.get(instanceInfo.getId());
        // If there are instance specific overrides, then they win - otherwise the ASG status
        // 这个状态就是admin提交给Server的状态，是admin最想让Server成为的状态，Server可以相信。
        // 如果overridden == null，可以表明一定是新注册的吗？也不一定，当client提交删除status请求，那么overriddenStatus会变为默认值unknown，
        // 此时statusOverridesMap中没有这个值，然后client如果又提交了注册请求，就是这种情况，这种情况也不是新注册的情况，而是删除后注册的情况。
        // do 确认一下status和overriddenStatus的关系，status就是Eureka Server维护的client端的状态，直接影响服务发现，而overriddenStatus
        // 是client 或者 admin期望修改Server维护的client的状态，类似于存了个临时变量，当client做注册or续约时，会重新计算status，这时候就在本类里面
        // 根据overriddenStatus重新计算status。
        if (overridden != null) {
            logger.debug("The instance specific override for instance {} and the value is {}",
                    instanceInfo.getId(), overridden.name());
            return StatusOverrideResult.matchingStatus(overridden);
        }
        return StatusOverrideResult.NO_MATCH;
    }

    @Override
    public String toString() {
        return OverrideExistsRule.class.getName();
    }

}
