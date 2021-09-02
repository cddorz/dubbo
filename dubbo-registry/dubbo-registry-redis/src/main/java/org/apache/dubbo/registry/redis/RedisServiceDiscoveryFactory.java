package org.apache.dubbo.registry.redis;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.AbstractServiceDiscoveryFactory;
import org.apache.dubbo.registry.client.ServiceDiscovery;


public class RedisServiceDiscoveryFactory extends AbstractServiceDiscoveryFactory {
    @Override
    protected ServiceDiscovery createDiscovery(URL registryURL) {
        return new RedisServiceDiscovery();
    }
}
