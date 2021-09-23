package org.apache.dubbo.registry.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.remoting.etcd.ChildListener;
import org.apache.dubbo.remoting.etcd.EtcdClient;

import com.google.gson.Gson;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.etcd.EtcdTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class EtcdServiceDiscovery extends AbstractServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(EtcdServiceDiscovery.class);

    private final String root = "/services";

    private final Set<String> services = new ConcurrentHashSet<>();

    EtcdClient etcdClient;

    private final Map<String, ChildListener> childListenerMap = new ConcurrentHashMap<>();

    private URL registryURL;
    @Override
    public void doInitialize(URL registryURL) throws Exception {
        EtcdTransporter etcdTransporter = ExtensionLoader.getExtensionLoader(EtcdTransporter.class).getAdaptiveExtension();
        etcdClient = etcdTransporter.connect(registryURL);
        this.registryURL = registryURL;
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) throws RuntimeException {
        try {
            String path = toPath(serviceInstance);
            etcdClient.putEphemeral(path,new Gson().toJson(serviceInstance));
            services.add(serviceInstance.getServiceName());
        }catch (Throwable e){
            throw new RpcException("Failed to registry " + serviceInstance.getServiceName() + " to etcd, cause: " + e.getMessage(),e);
        }
    }

    @Override
    public void doUpdate(ServiceInstance serviceInstance) throws RuntimeException {
        ServiceInstance oldInstance = this.serviceInstance;
        this.unregister(oldInstance);
        this.register(serviceInstance);
    }

    @Override
    public void doUnregister(ServiceInstance serviceInstance) {
        try {
            String path = toPath(serviceInstance);
            etcdClient.delete(path);
            services.remove(serviceInstance.getServiceName());
            this.serviceInstance = null;
        }catch (Throwable e){
            throw new RpcException("Failed to unregistry " + serviceInstance.getServiceName() + " from etcd, cause: " + e.getMessage(),e);
        }
    }

    @Override
    public URL getUrl() {
        return registryURL;
    }

    @Override
    public void doDestroy() throws Exception {
        if (etcdClient != null && etcdClient.isConnected()) {
            etcdClient.close();
        }
    }

    @Override
    public Set<String> getServices() {
        return Collections.unmodifiableSet(services);
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) {
        List<String> children = etcdClient.getChildren(toParentPath(serviceName));
        if (CollectionUtils.isEmpty(children)) {
            return Collections.EMPTY_LIST;
        }
        List<ServiceInstance> list = new ArrayList<>(children.size());
        for (String child : children) {
            ServiceInstance serviceInstance = new Gson().fromJson(etcdClient.getKVValue(child), DefaultServiceInstance.class);
            list.add(serviceInstance);
        }
        return list;
    }


    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
        listener.getServiceNames().forEach(serviceName -> registerServiceWatcher(serviceName,listener));
    }

    private void registerServiceWatcher(String serviceName,ServiceInstancesChangedListener listener) {
        String path = toParentPath(serviceName);
        ChildListener childListener =
                Optional.ofNullable(childListenerMap.get(serviceName))
                        .orElseGet(() -> {
                            ChildListener watchListener, prev;
                            prev = childListenerMap.putIfAbsent(serviceName, watchListener = (parentPath, currentChildren) ->
                                    dispatchServiceInstancesChangedEvent(serviceName));
                            return prev != null ? prev : watchListener;
                        });

        etcdClient.create(path);
        listener.onEvent(new ServiceInstancesChangedEvent(serviceName, this.getInstances(serviceName)));
        etcdClient.addChildListener(path, childListener);
    }

    String toPath(ServiceInstance serviceInstance) {
        return root + "/" + serviceInstance.getServiceName() + "/" + serviceInstance.getHost()
                + ":" + serviceInstance.getPort();
    }

    String toParentPath(String serviceName) {
        return root + "/" + serviceName;
    }
}
