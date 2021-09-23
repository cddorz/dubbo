package org.apache.dubbo.registry.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.AbstractServiceDiscoveryFactory;
import org.apache.dubbo.registry.client.ServiceDiscovery;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:53
 */
public class EtcdServiceDiscovertyFactory extends AbstractServiceDiscoveryFactory {
    @Override
    protected ServiceDiscovery createDiscovery(URL registryURL) {
        return null;
    }
}
