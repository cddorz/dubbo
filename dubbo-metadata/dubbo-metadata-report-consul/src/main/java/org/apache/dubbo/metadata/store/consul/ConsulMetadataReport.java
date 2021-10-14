package org.apache.dubbo.metadata.store.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.google.gson.Gson;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.identifier.*;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;


/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-09 20:59
 */
public class ConsulMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(ConsulMetadataReport.class);

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;

    private static final String DEFAULT_MAPPING_GROUP = "mapping";

    private Gson gson;

    private ConsulClient client;

    private Map<String, MappingDataListener> casListenerMap = new ConcurrentHashMap<>();
    private ExecutorService notifierExecutor = newCachedThreadPool(
        new NamedThreadFactory("dubbo-consul-notifier", true));


    public ConsulMetadataReport(URL registryURL){
        super(registryURL);
        String host = registryURL.getHost();
        int port = INVALID_PORT != registryURL.getPort() ? registryURL.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        this.storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
        this.storeMetadata(consumerMetadataIdentifier, serviceParameterString);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        this.storeMetadata(metadataIdentifier, URL.encode(url.toFullString()));
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        this.deleteMetadata(metadataIdentifier);
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        String content = getMetadata(metadataIdentifier);
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(Arrays.asList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        this.storeMetadata(subscriberMetadataIdentifier, urlListStr);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        return getMetadata(subscriberMetadataIdentifier);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return getMetadata(metadataIdentifier);
    }

    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo){
        String content = gson.toJson(metadataInfo);
        client.setKVValue(identifier.getApplication() + "-" + identifier.getRevision(),content);
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata){
        Response<GetValue> value = client.getKVValue(identifier.getApplication() + "-" + identifier.getRevision());
        if (value != null && value.getValue() != null) {
            String content = value.getValue().getValue();
            return gson.fromJson(content, MetadataInfo.class);
        }
        return null;
    }


    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        String path = buildPath(DEFAULT_MAPPING_GROUP,serviceKey);
        if(null == casListenerMap.get(serviceKey)){
            MappingDataListener mappingDataListener = casListenerMap.computeIfAbsent(serviceKey, k -> new MappingDataListener(serviceKey));
            mappingDataListener.addListener(listener);
        }
        ConsulNotifier consulNotifier = new ConsulNotifier(serviceKey);
        notifierExecutor.submit(consulNotifier);
        return getAppNames(getServiceAppNames(path));
    }

    @Override
    public boolean registerServiceAppMapping(String key, String group, String content, Object ticket){
        String path = buildPath(group,key);
        PutParams putParams = new PutParams();
        if(ticket != null){
            Response<GetValue> value = client.getKVValue(path);
            long cas = value.getValue().getModifyIndex();
            putParams.setCas(cas);
            client.setKVValue(path,content,putParams);
        }else {
            client.setKVValue(path,content);
        }
        return true;
    }
    @Override
    public ConfigItem getConfigItem(String serviceKey, String group) {
        String path = buildPath(group,serviceKey);
        String content = null;
        Response<GetValue> value = client.getKVValue(path);
        if (value != null && value.getValue() != null) {
            content = value.getValue().getDecodedValue();
        }
        return new ConfigItem(content,content);
    }


    private String buildPath(String group,String key){
        return group + "-" + key;
    }

    private void storeMetadata(BaseMetadataIdentifier identifier, String v) {
        try {
            client.setKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), v);
        } catch (Throwable t) {
            logger.error("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
            throw new RpcException("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
        }
    }

    private void deleteMetadata(BaseMetadataIdentifier identifier) {
        try {
            client.deleteKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable t) {
            logger.error("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }

    private String getServiceAppNames(String key) {
        Response<GetValue> value = client.getKVValue(key);
        if (value != null && value.getValue() != null) {
            return value.getValue().getDecodedValue();
        }
        return null;
    }

    private String getMetadata(BaseMetadataIdentifier identifier) {
        try {
            Response<GetValue> value = client.getKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            if (value != null && value.getValue() != null) {
                return value.getValue().getValue();
            }
            return null;
        } catch (Throwable t) {
            logger.error("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }


    private static class MappingDataListener {

        private String serviceKey;
        private Set<MappingListener> listeners;

        public MappingDataListener(String serviceKey) {
            this.serviceKey = serviceKey;
            this.listeners = new HashSet<>();
        }

        public void addListener(MappingListener listener) {
            this.listeners.add(listener);
        }
    }

    private class ConsulNotifier implements  Runnable{
        private boolean running;
        private MappingDataListener mappingListener;
        String key;

        public ConsulNotifier(String key){
            this.mappingListener = casListenerMap.computeIfAbsent(key, k -> new MappingDataListener(key));
            this.key = key;

        }
        @Override
        public void run() {
            while (this.running){
                Set<String> apps = getAppNames(getServiceAppNames(key));
                MappingChangedEvent event = new MappingChangedEvent(key,apps);
                mappingListener.listeners.forEach(mappingListener -> mappingListener.onEvent(event));
            }
        }

        public void stop(){
            this.running = false;
        }
    }
}
