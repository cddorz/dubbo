package org.apache.dubbo.registry.eureka;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;


import com.netflix.appinfo.ApplicationInfoManager;
import com.netflix.appinfo.EurekaInstanceConfig;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.config.ConfigurationManager;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.DefaultEurekaClientConfig;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaClientConfig;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.shared.Application;
import com.netflix.discovery.shared.Applications;
import javafx.event.EventDispatcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.emptyList;
import static org.apache.dubbo.common.constants.RegistryConstants.SUBSCRIBED_SERVICE_NAMES_KEY;


/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-26 15:41
 */
public class EurekaServiceDiscovery extends AbstractServiceDiscovery {


    private ApplicationInfoManager applicationInfoManager;

    private EurekaClient eurekaClient;

    private String lastAppsHashCode;

    private ServiceInstancesChangedListener listener;

    private String serviceName;

    @Override
    public void doInitialize(URL registryURL) throws Exception {
        Properties properties = buildEurekaConfigProperties(registryURL);
        initConfigurationManager(properties);
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance) throws RuntimeException {
        initEurekaClient(serviceInstance);
        setInstanceStatus(InstanceInfo.InstanceStatus.UP);
    }

    @Override
    public void doUpdate(ServiceInstance serviceInstance) throws RuntimeException {
        setInstanceStatus(serviceInstance.isHealthy() ? InstanceInfo.InstanceStatus.UP :
                InstanceInfo.InstanceStatus.UNKNOWN);
    }

    @Override
    public void doUnregister(ServiceInstance serviceInstance) {
        setInstanceStatus(InstanceInfo.InstanceStatus.OUT_OF_SERVICE);
    }

    @Override
    public void doDestroy() throws Exception {
        eurekaClient.shutdown();
    }

    @Override
    public Set<String> getServices() {
        Applications applications = eurekaClient.getApplications();
        if(applications == null){
            return Collections.emptySet();
        }
        List<Application> registeredApplications = applications.getRegisteredApplications();
        Set<String> services = new LinkedHashSet<>();
        for (Application app : registeredApplications){
            if(app.getInstances().isEmpty()){
                continue;
            }
            services.add(app.getName().toLowerCase());
        }
        return services;
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) throws NullPointerException {
        Application application = eurekaClient.getApplication(serviceName);

        if(application == null){
            return emptyList();
        }
        List<InstanceInfo> infos = application.getInstances();
        List<ServiceInstance> instances = new ArrayList<>();
        for (InstanceInfo info : infos){
            instances.add(buildServiceInstance(info));
        }
        return instances;
    }

    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener){
        listener.getServiceNames().forEach(serviceName -> registerServiceWatcher(serviceName,listener));
    }

    private void registerServiceWatcher(String serviceName,ServiceInstancesChangedListener listener){
        registerEurekaEventListener(eurekaClient);
        this.listener = listener;
        this.serviceName = serviceName;
    }

    private void registerEurekaEventListener(EurekaClient eurekaClient) {
        eurekaClient.registerEventListener(this::onEurekaEvent);
    }

    private void onEurekaEvent(EurekaEvent event) {
        if (event instanceof CacheRefreshedEvent) {
            onCacheRefreshedEvent(CacheRefreshedEvent.class.cast(event));
        }
    }

    private void onCacheRefreshedEvent(CacheRefreshedEvent event) {
        synchronized (this) {
            Applications applications = eurekaClient.getApplications();
            String appsHashCode = applications.getAppsHashCode();
            if (!Objects.equals(lastAppsHashCode, appsHashCode)) {
                lastAppsHashCode = appsHashCode;
                listener.onEvent(new ServiceInstancesChangedEvent(serviceName,getInstances(serviceName)));
            }
        }
    }

    private void setInstanceStatus(InstanceInfo.InstanceStatus status) {
        if (applicationInfoManager != null) {
            this.applicationInfoManager.setInstanceStatus(status);
        }
    }

    private Properties buildEurekaConfigProperties(URL registryURL) {
        Properties properties = new Properties();
        Map<String, String> parameters = registryURL.getParameters();
        setDefaultProperties(registryURL, properties);
        parameters.entrySet().stream()
                .filter(this::filterEurekaProperty)
                .forEach(propertyEntry -> properties.setProperty(propertyEntry.getKey(), propertyEntry.getValue()));
        return properties;
    }

    private boolean filterEurekaProperty(Map.Entry<String, String> propertyEntry) {
        String propertyName = propertyEntry.getKey();
        return propertyName.startsWith("eureka.");
    }

    private void setDefaultProperties(URL registryURL, Properties properties) {
        setDefaultServiceURL(registryURL, properties);
    }

    private void setDefaultServiceURL(URL registryURL, Properties properties) {
        StringBuilder defaultServiceURLBuilder = new StringBuilder("http://")
                .append(registryURL.getHost())
                .append(":")
                .append(registryURL.getPort())
                .append("/eureka");
        properties.setProperty("eureka.serviceUrl.default", defaultServiceURLBuilder.toString());
    }


    private void initConfigurationManager(Properties eurekaConfigProperties) {
        ConfigurationManager.loadProperties(eurekaConfigProperties);
    }

    private void initApplicationInfoManager(ServiceInstance serviceInstance) {
        EurekaInstanceConfig eurekaInstanceConfig = buildEurekaInstanceConfig(serviceInstance);
        this.applicationInfoManager = new ApplicationInfoManager(eurekaInstanceConfig, (ApplicationInfoManager.OptionalArgs) null);
    }

    private void initEurekaClient(ServiceInstance serviceInstance) {
        if (eurekaClient != null) {
            return;
        }
        initApplicationInfoManager(serviceInstance);
        EurekaClient eurekaClient = createEurekaClient();
        // set eurekaClient
        this.eurekaClient = eurekaClient;
    }

    private EurekaClient createEurekaClient() {
        EurekaClientConfig eurekaClientConfig = new DefaultEurekaClientConfig();
        DiscoveryClient eurekaClient = new DiscoveryClient(applicationInfoManager, eurekaClientConfig);
        return eurekaClient;
    }
    private ServiceInstance buildServiceInstance(InstanceInfo instance) {
        DefaultServiceInstance serviceInstance = new DefaultServiceInstance(instance.getAppName(),
                instance.getHostName(),
                instance.isPortEnabled(InstanceInfo.PortType.SECURE) ? instance.getSecurePort() : instance.getPort());
        serviceInstance.setMetadata(instance.getMetadata());
        return serviceInstance;
    }

    private EurekaInstanceConfig buildEurekaInstanceConfig(ServiceInstance serviceInstance) {
        ConfigurableEurekaInstanceConfig eurekaInstanceConfig = new ConfigurableEurekaInstanceConfig()
                .setInstanceId(serviceInstance.getAddress())
                .setAppname(serviceInstance.getServiceName())
                .setIpAddress(serviceInstance.getHost())
                .setNonSecurePort(serviceInstance.getPort())
                .setMetadataMap(serviceInstance.getMetadata());
        return eurekaInstanceConfig;
    }


}


