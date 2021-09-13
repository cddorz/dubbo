package org.apache.dubbo.registry.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.catalog.CatalogServicesRequest;
import com.ecwid.consul.v1.health.HealthServicesRequest;
import com.ecwid.consul.v1.health.model.HealthService;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
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
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SEPARATOR_CHAR;
import static org.apache.dubbo.common.constants.CommonConstants.SEMICOLON_SPLIT_PATTERN;
import static org.apache.dubbo.registry.consul.ConsulParameter.DEFAULT_ZONE_METADATA_NAME;
import static org.apache.dubbo.registry.consul.ConsulParameter.INSTANCE_GROUP;
import static org.apache.dubbo.registry.consul.ConsulParameter.INSTANCE_ZONE;
import static org.apache.dubbo.registry.consul.ConsulParameter.TAGS;

public class ConsulServiceDiscovery extends AbstractServiceDiscovery {

    private static final Logger logger = LoggerFactory.getLogger(ConsulServiceDiscovery.class);

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;

    private URL registryURL;

    private static final int DEFAULT_WATCH_TIMEOUT = 60 * 1000;

    private static final String REGISTER_TAG = "consul_register_tag";

    private static final String WATCH_TIMEOUT = "consul-watch-timeout";

    private static final int DEFAULT_INDEX = -1;
    private static final int DEFAULT_WAIT_TIME = -1;

    private ConsulClient client;
    private long checkPassInterval;
    private TtlScheduler ttlScheduler;
    private String dRegistryTime;
    private String token;
    private int watchTimeout;
    private ExecutorService notifierExecutor = newCachedThreadPool(
        new NamedThreadFactory("dubbo-consul-notifier", true));
    private ConcurrentMap<String,ConsulNotifier> notifiers = new ConcurrentHashMap<>();

    private List<String> tags;

    private List<String> registeringTags = new ArrayList<>();


    private String defaultZoneMetadataName;

    private String instanceZone;

    private String instanceGroup;


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
        ttlScheduler = new TtlScheduler(checkPassInterval, client);
        this.registeringTags.addAll(getRegisteringTags(registryURL));
        this.tags = getTags(registryURL);
        this.defaultZoneMetadataName = DEFAULT_ZONE_METADATA_NAME.getValue(registryURL);
        this.instanceZone = INSTANCE_ZONE.getValue(registryURL);
        this.instanceGroup = INSTANCE_GROUP.getValue(registryURL);
    }

    private List<String> getTags(URL registryURL) {
        String value = TAGS.getValue(registryURL);
        return StringUtils.splitToList(value, COMMA_SEPARATOR_CHAR);
    }

    private List<String> getRegisteringTags(URL url) {
        List<String> tags = new ArrayList<>();
        String rawTag = url.getParameter(REGISTER_TAG);
        if (StringUtils.isNotEmpty(rawTag)) {
            tags.addAll(Arrays.asList(SEMICOLON_SPLIT_PATTERN.split(rawTag)));
        }
        return tags;
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) {
        try {
            NewService consulService = buildService(serviceInstance);
            if(null == token){
                client.agentServiceRegister(consulService);
                ttlScheduler.add(consulService.getId());
            }else {
                client.agentServiceRegister(consulService,token);
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
        notifiers.forEach((_k, notifier) -> {
            if (notifier != null) {
                notifier.stop();
            }
        });
        ttlScheduler.stop();
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
        return convert(response.getValue());
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
        return services.stream()
            .map(HealthService::getService)
            .map(service -> {
                ServiceInstance instance = new DefaultServiceInstance(
                    service.getService(),
                    service.getAddress(),
                    service.getPort());
                instance.getMetadata().putAll(getMetadata(service));
                return instance;
            })
            .collect(Collectors.toList());
    }

    private Map<String, String> getMetadata(HealthService.Service service) {
        return getScCompatibleMetadata(service.getTags());
    }

    private Map<String, String> getScCompatibleMetadata(List<String> tags) {
        LinkedHashMap<String, String> metadata = new LinkedHashMap<>();
        if (tags != null) {
            for (String tag : tags) {
                String[] parts = StringUtils.delimitedListToStringArray(tag, "=");
                switch (parts.length) {
                    case 0:
                        break;
                    case 1:
                        metadata.put(parts[0], parts[0]);
                        break;
                    case 2:
                        metadata.put(parts[0], parts[1]);
                        break;
                    default:
                        String[] end = Arrays.copyOfRange(parts, 1, parts.length);
                        metadata.put(parts[0], StringUtils.arrayToDelimitedString(end, "="));
                        break;
                }

            }
        }
        return metadata;
    }

    private NewService buildService(ServiceInstance serviceInstance){
        NewService service = new NewService();
        service.setName(serviceInstance.getServiceName());
        service.setAddress(serviceInstance.getAddress());
        service.setPort(serviceInstance.getPort());
        service.setId(buildId(serviceInstance));
        service.setTags(buildTags(serviceInstance));
        service.setCheck(buildCheck());
        return service;
    }

    private String buildId(ServiceInstance serviceInstance){
        return Integer.toString(serviceInstance.hashCode());
    }

    private List<String> buildTags(ServiceInstance serviceInstance){
        List<String> tags = new LinkedList<>(this.tags);

        if (StringUtils.isNotEmpty(instanceZone)) {
            tags.add(defaultZoneMetadataName + "=" + instanceZone);
        }

        if (StringUtils.isNotEmpty(instanceGroup)) {
            tags.add("group=" + instanceGroup);
        }

        Map<String, String> params = serviceInstance.getMetadata();
        params.keySet().stream()
            .map(k -> k + "=" + params.get(k))
            .forEach(tags::add);

        tags.addAll(registeringTags);
        return tags;
    }

    private NewService.Check buildCheck() {
        NewService.Check check = new NewService.Check();
        check.setTtl((checkPassInterval / ONE_THOUSAND) + "s");
        check.setDeregisterCriticalServiceAfter(dRegistryTime);
        return check;
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

    private static class TtlScheduler {

        private static final Logger logger = LoggerFactory.getLogger(TtlScheduler.class);

        private final Map<String, ScheduledFuture> serviceHeartbeats = new ConcurrentHashMap<>();

        private ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        private long checkInterval;

        private ConsulClient client;

        public TtlScheduler(long checkInterval, ConsulClient client) {
            this.checkInterval = checkInterval;
            this.client = client;
        }

        public void add(String instanceId) {
            ScheduledFuture task = this.scheduler.scheduleAtFixedRate(
                new ConsulHeartbeatTask(instanceId),
                checkInterval / PERIOD_DENOMINATOR,
                checkInterval / PERIOD_DENOMINATOR,
                TimeUnit.MILLISECONDS);
            ScheduledFuture previousTask = this.serviceHeartbeats.put(instanceId, task);
            if (previousTask != null) {
                previousTask.cancel(true);
            }
        }


        private class ConsulHeartbeatTask implements Runnable {

            private String checkId;

            ConsulHeartbeatTask(String serviceId) {
                this.checkId = serviceId;
                if (!this.checkId.startsWith("service:")) {
                    this.checkId = "service:" + this.checkId;
                }
            }

            @Override
            public void run() {
                TtlScheduler.this.client.agentCheckPass(this.checkId);
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending consul heartbeat for: " + this.checkId);
                }
            }

        }

        public void stop() {
            scheduler.shutdownNow();
        }

    }
}

