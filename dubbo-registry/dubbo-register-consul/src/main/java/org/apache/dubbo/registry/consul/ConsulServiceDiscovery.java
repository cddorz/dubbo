package org.apache.dubbo.registry.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;

import java.util.Set;

public class ConsulServiceDiscovery extends AbstractServiceDiscovery {
    ConsulServiceDiscovery(URL url){}

    @Override
    public void doInitialize(URL registryURL) throws Exception {

    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) throws RuntimeException {

    }

    @Override
    public void doUpdate(ServiceInstance serviceInstance) throws RuntimeException {

    }

    @Override
    public void doUnregister(ServiceInstance serviceInstance) {

    }

    @Override
    public void doDestroy() throws Exception {

    }

    @Override
    public Set<String> getServices() {
        return null;
    }
}

