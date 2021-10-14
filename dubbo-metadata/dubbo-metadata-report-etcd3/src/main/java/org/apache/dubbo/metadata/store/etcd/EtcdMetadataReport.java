package org.apache.dubbo.metadata.store.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.identifier.BaseMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import com.google.gson.Gson;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;

import static com.google.common.base.Charsets.UTF_8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.metadata.ServiceNameMapping.DEFAULT_MAPPING_GROUP;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;

public class EtcdMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(EtcdMetadataReport.class);

    private final String root;

    private Client client;
    private KV kvClient;

    private Gson gson;

    private Map<String, Watch.Watcher> watcherMap = new ConcurrentHashMap<>();

    private Map<String, MappingDataListener> casListenerMap = new ConcurrentHashMap<>();


    public EtcdMetadataReport(URL url) {
        super(url);
        client = Client.builder().endpoints("http://127.0.0.1:2379").build();
        kvClient = client.getKVClient();
        String group = url.getGroup(DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
    }

    protected String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private static ByteSequence bytesOf(String val) {
        return ByteSequence.from(val,UTF_8);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        try {
            GetResponse response = kvClient.get(bytesOf(getNodePath(metadataIdentifier))).get();
            return response.getKvs().get(0).getValue().toString(UTF_8);
        }catch (Throwable r){
            throw new RpcException("Failed to get serviceDefinition from etcd, cause: " + r.getMessage(),r);
        }
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
        storeMetadata(consumerMetadataIdentifier,serviceParameterString);
    }

    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo){
        String content = gson.toJson(metadataInfo);
        try {
            kvClient.put(bytesOf(identifier.getApplication() + "-" + identifier.getRevision()),bytesOf(content)).get();
        }catch (Throwable e){
            throw new RpcException(e.getMessage(),e);
        }
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata){
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(bytesOf(identifier.getApplication() + "-" + identifier.getRevision()));
            GetResponse response = getFuture.get();
            String string = response.getKvs().get(0).getValue().toString(UTF_8);
            return gson.fromJson(string,MetadataInfo.class);
        }catch (Throwable e){
            throw new RpcException(e.getMessage(),e);
        }
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        try {
            kvClient.put(bytesOf(getNodePath(metadataIdentifier)), bytesOf(URL.encode(url.toFullString()))).get();
        } catch (Exception e) {
            throw new RpcException("Failed to save meta from etcd,cause: " + e.getMessage(),e);
        }
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        try {
            kvClient.delete(bytesOf(getNodePath(metadataIdentifier))).get();
        } catch (Exception e) {
            throw new RpcException("Failed to delete meta from etcd,cause: " + e.getMessage(),e);
        }
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(bytesOf(getNodePath(metadataIdentifier)));
            GetResponse response = getFuture.get();
            String value = response.getKvs().get(0).getValue().toString(UTF_8);
            if (StringUtils.isEmpty(value)) {
                return Collections.emptyList();
            }
            return new ArrayList<>(Collections.singletonList(URL.decode(value)));
        }catch (Throwable r){
            logger.warn("Failed to get exportedUrls from etcd, cause: " + r.getMessage(),r);
            throw new RpcException("Failed to get exportedUrls from etcd, cause: " + r.getMessage(),r);
        }
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        try {
            kvClient.put(bytesOf(getNodePath(subscriberMetadataIdentifier)),bytesOf(urlListStr)).get();
        }catch (Throwable t){
            logger.warn("Failed to save subscriberData from etcd, cause: " + t.getMessage(),t);
            throw new RpcException("Failed to save subscriberData from etcd, cause: " + t.getMessage(),t);
        }
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        try {
            CompletableFuture<GetResponse> getFuture = kvClient.get(bytesOf(getNodePath(subscriberMetadataIdentifier)));
            GetResponse response = getFuture.get();
            return response.getKvs().get(0).getValue().toString(UTF_8);
        }catch (Throwable t){
            logger.warn("Failed to get subscriberData from etcd, cause: " + t.getMessage(),t);
            throw new RpcException("Failed to get subscriberData from etcd, cause: " + t.getMessage(),t);
        }
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey,MappingListener listener,URL url){
        String path = buildPathKey(DEFAULT_MAPPING_GROUP, serviceKey);
        if (null == casListenerMap.get(path)) {
            addCasServiceMappingListener(path, listener,serviceKey);
        }
        try {
            GetResponse response = kvClient.get(bytesOf(path)).get();
            String content = response.getKvs().get(0).getValue().toString(UTF_8);
            return getAppNames(content);
        }catch (Throwable r){
            throw new RpcException("Failed to getConfigItem from etcd,cause: " + r.getMessage(),r);
        }
    }

    @Override
    public ConfigItem getConfigItem(String serviceKey, String group) {
        String content = null;
        String path = buildPathKey(group,serviceKey);
        try {
            GetResponse response = kvClient.get(bytesOf(path)).get();
            if(response != null && response.getKvs().size() != 0){
                content = response.getKvs().get(0).getValue().toString(UTF_8);
            }
            return new ConfigItem(content,content);
        }catch (Throwable r){
            throw new RpcException("Failed to getConfigItem from etcd,cause: " + r.getMessage(),r);
        }
    }

    @Override
    public boolean registerServiceAppMapping(String key, String group, String content, Object ticket) {
        try {
            String path = buildPathKey(group,key);
            if(null != ticket){
                ByteSequence casKey = bytesOf(path);
                ByteSequence oldKey = bytesOf((String) ticket);
                ByteSequence newKey = bytesOf(content);
                Cmp cmp = new Cmp(casKey, Cmp.Op.EQUAL, CmpTarget.value(oldKey));
                TxnResponse txnResponse = client.getKVClient()
                        .txn()
                        .If(cmp)
                        .Then(Op.put(casKey, newKey , PutOption.DEFAULT))
                        .commit()
                        .get();
                return txnResponse.isSucceeded() && CollectionUtils.isNotEmpty(txnResponse.getPutResponses());
            }else {
                kvClient.put(bytesOf(path),bytesOf(content)).get();
                return true;
            }
        }catch (Throwable r){
            throw new RpcException("Failed to registry serviceAppMapping from etcd,cause: " + r.getMessage(),r);
        }
    }

    private void addCasServiceMappingListener(String path, MappingListener listener,String serviceKey){
        MappingDataListener mappingDataListener = casListenerMap.computeIfAbsent(path, _k -> new MappingDataListener());
        mappingDataListener.addListener(listener,path,serviceKey);
    }

    private void storeMetadata(MetadataIdentifier metadataIdentifier, String v) {
        try {
            kvClient.put(bytesOf(getNodePath(metadataIdentifier)),bytesOf(v)).get();
        }catch (Throwable r){
            throw new RpcException("Failed ot store metadata from etcd, cause: " + r.getMessage(),r);
        }

    }

    private String buildPathKey(String group, String serviceKey) {
        return toRootDir() + group + PATH_SEPARATOR + serviceKey;
    }

    private String getNodePath(BaseMetadataIdentifier metadataIdentifier) {
        return toRootDir() + metadataIdentifier.getUniqueKey(KeyTypeEnum.PATH);
    }

    private  class MappingDataListener{
        private Set<MappingListener> listeners;

        public MappingDataListener(){
            listeners = new HashSet<>();
        }

        public Watch.Watcher watch(String pathKey,Watch.Listener listener){
            return client.getWatchClient().watch(bytesOf(pathKey),listener);
        }

        public void addListener(MappingListener listener,String path,String serviceKey){
            listeners.add(listener);
            Watch.Listener watch_listener = Watch.listener(watchResponse -> {

                // 被调用时传入的是事件集合，这里遍历每个事件
                watchResponse.getEvents().forEach(watchEvent -> {
                    // 操作类型
                    WatchEvent.EventType eventType = watchEvent.getEventType();

                    // 操作的键值对
                    KeyValue keyValue = watchEvent.getKeyValue();
                    if(keyValue.getKey().toString(UTF_8).equals(path)){
                        Set<String> apps = getAppNames(keyValue.getValue().toString(UTF_8));
                        MappingChangedEvent event = new MappingChangedEvent(serviceKey, apps);
                        listeners.forEach(mappingListener -> mappingListener.onEvent(event));
                    }
                    // 如果是删除操作，就把该key的Watcher找出来close掉
                    if (WatchEvent.EventType.DELETE.equals(eventType)
                            && watcherMap.containsKey(path)) {
                        Watch.Watcher watcher = watcherMap.remove(path);
                        watcher.close();
                    }

                });
            });
            Watch.Watcher watcher = watch(path,watch_listener);
            watcherMap.put(path,watcher);
        }

    }


}
