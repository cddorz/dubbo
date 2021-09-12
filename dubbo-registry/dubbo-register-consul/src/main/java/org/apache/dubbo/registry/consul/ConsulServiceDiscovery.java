package org.apache.dubbo.registry.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.rpc.RpcException;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.dubbo.registry.consul.AbstractConsulRegistry.*;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

public class ConsulServiceDiscovery extends AbstractServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ConsulServiceDiscovery.class);

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;

    private URL registryURL;

    private static final int DEFAULT_WATCH_TIMEOUT = 60 * 1000;

    private static final String WATCH_TIMEOUT = "consul-watch-timeout";

    private static final int DEFAULT_INDEX = -1;
    private static final int DEFAULT_WAIT_TIME = -1;

    private ConsulClient client;
    private long checkPassInterval;
    private String dRegistryTime;
    private String token;
    private int watchTimeout;
    private ExecutorService notifierExecutor = newCachedThreadPool(
        new NamedThreadFactory("dubbo-consul-notifier", true));
    private ConcurrentMap<String,ConsulNotifier> notifiers = new ConcurrentHashMap<>();

    @Override
    public void doInitialize(URL registryURL) throws Exception {
        String host = registryURL.getHost();
        int port = INVALID_PORT != registryURL.getPort() ? registryURL.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
        token = registryURL.getParameter(TOKEN_KEY, (String) null);
        dRegistryTime = registryURL.getParameter(DEREGISTER_AFTER, DEFAULT_DEREGISTER_TIME);
        checkPassInterval = registryURL.getParameter(CHECK_PASS_INTERVAL, DEFAULT_CHECK_PASS_INTERVAL);
        watchTimeout = registryURL.getParameter(WATCH_TIMEOUT, DEFAULT_WATCH_TIMEOUT) / ONE_THOUSAND;
        this.registryURL = registryURL;
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) {
        try {
            if(null == token){
                client.agentServiceRegister(buildService(serviceInstance));
            }else {
                client.agentServiceRegister(buildService(serviceInstance),token);
            }
        }catch (Exception e){
            logger.warn("Failed to registry serviceInstance from consul, cause:" + e.getMessage(),e);
            throw new RpcException("Failed to registry serviceInstance from consul, cause:" + e.getMessage(), e);
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
            if (token == null) {
                client.agentServiceDeregister(buildId(serviceInstance));
            } else {
                client.agentServiceDeregister(buildId(serviceInstance), token);
            }
        }catch (Exception e){
            logger.warn("Failed to unregistry serviceInstance from consul, cause:"+ e.getMessage() , e);
            throw new RpcException("Failed to unregistry serviceInstance from consul, cause:"+ e.getMessage() , e);
        }
    }

    @Override
    public void doDestroy() throws Exception {
        notifierExecutor.shutdown();
    }

    @Override
    public URL getUrl(){
        return registryURL;
    }

    @Override
    public Set<String> getServices() {
        Response<Map<String, List<String>>> response = getAllServices(DEFAULT_INDEX, watchTimeout);
        List<HealthService> services = getHealthServices(response.getValue());
        return services.stream()
            .map(HealthService::getService)
            .filter(Objects::nonNull)
            .map(HealthService.Service::getService)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) throws NullPointerException {
        Response<List<HealthService>> response = getHealthServices(serviceName, DEFAULT_INDEX, watchTimeout);
        List<ServiceInstance> serviceInstances = convert(response.getValue());
        return serviceInstances;
    }


    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener){
        listener.getServiceNames().forEach(serviceName -> registerServiceWatcher(serviceName, listener));
    }

    public void registerServiceWatcher(String serviceName,ServiceInstancesChangedListener listener){
        ConsulNotifier notifier = notifiers.computeIfAbsent(serviceName,k -> new ConsulNotifier(listener,serviceName));
        notifier.run();
        notifierExecutor.submit(notifier);
    }


    private List<ServiceInstance> convert(List<HealthService> services){
        Gson gson = new GsonBuilder().create();
        List<ServiceInstance> serviceInstances = new ArrayList<>();
        List<String> lists = services.stream()
            .map(HealthService::getService)
            .filter(Objects::nonNull)
            .map(HealthService.Service::getMeta)
            .filter(m -> m != null && m.containsKey(SERVICEINSTANCE_META_KEY))
            .map(m -> m.get(SERVICEINSTANCE_META_KEY))
            .collect(Collectors.toList());
        for(String serviceInstanceToString : lists){
            ServiceInstance serviceInstance = gson.fromJson(serviceInstanceToString, DefaultServiceInstance.class);
            serviceInstances.add(serviceInstance);
        }
        return serviceInstances;
    }

    private NewService buildService(ServiceInstance serviceInstance){
        NewService service = new NewService();
        service.setName(serviceInstance.getServiceName());
        service.setAddress(serviceInstance.getAddress());
        service.setPort(serviceInstance.getPort());
        service.setId(buildId(serviceInstance));
        service.setTags(buildTags(serviceInstance));
        service.setCheck(buildCheck());
        service.setMeta(buildMeta(serviceInstance));
        return service;
    }

    private String buildId(ServiceInstance serviceInstance){
        return Integer.toString(serviceInstance.hashCode());
    }

    private List<String> buildTags(ServiceInstance serviceInstance){
        Map<String, String> params = serviceInstance.getAllParams();
        List<String> tags = params.entrySet().stream()
            .map(k -> k.getKey() + "=" + k.getValue())
            .collect(Collectors.toList());
        tags.add(SERVICE_TAG);
        return tags;
    }

    private NewService.Check buildCheck() {
        NewService.Check check = new NewService.Check();
        check.setTtl((checkPassInterval / ONE_THOUSAND) + "s");
        check.setDeregisterCriticalServiceAfter(dRegistryTime);
        return check;
    }

    private Map<String, String> buildMeta(ServiceInstance serviceInstance){
        Map<String, String> map = new Hashtable<>();
        Gson gson = new GsonBuilder().create();
        String value = gson.toJson(serviceInstance);
        map.put(SERVICEINSTANCE_META_KEY,value);
        return map;
    }

    private Response<List<HealthService>> getHealthServices(String service, long index, int watchTimeout) {
        HealthServicesRequest request = HealthServicesRequest.newBuilder()
            .setTag(SERVICE_TAG)
            .setQueryParams(new QueryParams(watchTimeout, index))
            .setPassing(true)
            .setToken(token)
            .build();
        return client.getHealthServices(service, request);
    }

    private Response<Map<String, List<String>>> getAllServices(long index, int watchTimeout) {
        CatalogServicesRequest request = CatalogServicesRequest.newBuilder()
            .setQueryParams(new QueryParams(watchTimeout, index))
            .setToken(token)
            .build();
        return client.getCatalogServices(request);
    }

    private List<HealthService> getHealthServices(Map<String, List<String>> services) {
        return services.entrySet().stream()
            .filter(s -> s.getValue().contains(SERVICE_TAG))
            .map(s -> getHealthServices(s.getKey(), DEFAULT_INDEX, DEFAULT_WAIT_TIME).getValue())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    }

    private class ConsulNotifier implements  Runnable{
        private boolean running;
        private ServiceInstancesChangedListener listener;
        private String serviceName;

        ConsulNotifier(ServiceInstancesChangedListener listener,String serviceName){
            this.listener = listener;
            this.serviceName = serviceName;
        }
        @Override
        public void run() {
            while (this.running){
                listener.onEvent(new ServiceInstancesChangedEvent(serviceName,getInstances(serviceName)));
            }
        }

        public void stop(){
            this.running = false;
        }
    }
}

