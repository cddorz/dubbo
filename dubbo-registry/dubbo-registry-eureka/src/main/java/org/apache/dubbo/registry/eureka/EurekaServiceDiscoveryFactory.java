package org.apache.dubbo.registry.eureka;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.AbstractServiceDiscoveryFactory;
import org.apache.dubbo.registry.client.ServiceDiscovery;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-26 15:41
 */
public class EurekaServiceDiscoveryFactory extends AbstractServiceDiscoveryFactory {
    @Override
    protected ServiceDiscovery createDiscovery(URL registryURL) {
        return new EurekaServiceDiscovery();
    }
}
