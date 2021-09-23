package org.apache.dubbo.configcenter.support.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.TreePathDynamicConfiguration;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.StringUtils;

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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.dubbo.common.constants.CommonConstants.DOT_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;

public class EtcdDynamicConfiguration extends TreePathDynamicConfiguration {

    private String rootPath;
    private Client client;
    private KV kvClient;

    private final Set<String> services = new ConcurrentHashSet<>();

    private Map<String, Watch.Watcher> watcherMap = new ConcurrentHashMap<>();

    private Map<String, ConfigurationDataListener> casListenerMap = new ConcurrentHashMap<>();

    public EtcdDynamicConfiguration(URL url) {
        super(url);
        rootPath = getRootPath(url);
        client = Client.builder().endpoints("http://127.0.0.1:2379").build();
        kvClient = client.getKVClient();
    }

    private static ByteSequence bytesOf(String val) {
        return ByteSequence.from(val,UTF_8);
    }

    @Override
    public String getInternalProperty(String key){
        try {
            GetResponse response = kvClient.get(bytesOf(buildPathKey("",key))).get();
            return response.getKvs().get(0).getValue().toString(UTF_8);
        } catch(Throwable r){
            throw new RuntimeException("Failed to get internalProperty from etcd,cause: " + r.getMessage(),r);
        }
    }

    @Override
    protected void doClose() throws Exception {
        client.close();
    }

    @Override
    protected boolean doPublishConfig(String pathKey, String content) throws Exception {
        services.add(pathKey);
        kvClient.put(bytesOf(pathKey),bytesOf(content)).get();
        return true;
    }

    @Override
    protected String doGetConfig(String pathKey) throws Exception {
        GetResponse response = kvClient.get(bytesOf(pathKey)).get();
        return response.getKvs().get(0).getValue().toString(UTF_8);
    }

    @Override
    protected boolean doRemoveConfig(String pathKey) throws Exception {
        kvClient.delete(bytesOf(pathKey)).get();
        return true;
    }

    @Override
    protected Collection<String> doGetConfigKeys(String groupPath) {
        return services;
    }

    @Override
    protected void doAddListener(String pathKey, ConfigurationListener listener) {
        ConfigurationDataListener configurationDataListener = casListenerMap.computeIfAbsent(pathKey,k->new ConfigurationDataListener());
        configurationDataListener.addListener(listener,pathKey);
    }

    @Override
    protected void doRemoveListener(String pathKey, ConfigurationListener listener) {
        ConfigurationDataListener configurationDataListener = casListenerMap.get(pathKey);
        configurationDataListener.removeListener(listener);
        casListenerMap.remove(pathKey);
    }

    @Override
    public boolean publishConfigCas(String key, String group, String content, Object ticket) {
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
            throw new RuntimeException("Failed to publish config cas from etcd,cause: " + r.getMessage(),r);
        }
    }

    @Override
    public ConfigItem getConfigItem(String key, String group) {
        try {
            String path = buildPathKey(group,key);
            GetResponse response = kvClient.get(bytesOf(path)).get();
            String content = response.getKvs().get(0).getValue().toString(UTF_8);
            return new ConfigItem(content,content);
        }catch (Throwable r){
            throw new RuntimeException("Failed to get configItem from etcd,cause: " + r.getMessage(),r);
        }
    }


    private String pathToKey(String path) {
        if (StringUtils.isEmpty(path)) {
            return path;
        }
        String groupKey = path.replace(rootPath + PATH_SEPARATOR, "").replaceAll(PATH_SEPARATOR, DOT_SEPARATOR);
        return groupKey.substring(groupKey.indexOf(DOT_SEPARATOR) + 1);
    }

    private String getGroup(String path) {
        if (!StringUtils.isEmpty(path)) {
            int beginIndex = path.indexOf(rootPath + PATH_SEPARATOR);
            if (beginIndex > -1) {
                String remain = path.substring((rootPath + PATH_SEPARATOR).length());
                int endIndex = remain.lastIndexOf(PATH_SEPARATOR);
                if (endIndex > -1) {
                    return remain.substring(0, endIndex);
                }
            }
        }
        return path;
    }

    private class ConfigurationDataListener{
        private Set<ConfigurationListener> listeners;

        public ConfigurationDataListener(){
            listeners = new HashSet<>();
        }

        private Watch.Watcher watch(String pathKey,Watch.Listener listener){
            return client.getWatchClient().watch(bytesOf(pathKey),listener);
        }

        public void addListener(ConfigurationListener listener,String path){
            listeners.add(listener);
            Watch.Listener watch_listener = Watch.listener(watchResponse -> {
                // 被调用时传入的是事件集合，这里遍历每个事件
                watchResponse.getEvents().forEach(watchEvent -> {
                    // 操作类型
                    WatchEvent.EventType eventType = watchEvent.getEventType();
                    // 操作的键值对
                    KeyValue keyValue = watchEvent.getKeyValue();
                    if(path.equals(keyValue.getKey().toString(UTF_8))){
                        String key = pathToKey(path);
                        String group = getGroup(path);
                        try {
                            ConfigChangedEvent configChangedEvent = new ConfigChangedEvent(key,group,doGetConfig(path));
                            listeners.forEach(l -> l.process(configChangedEvent));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
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

        public void removeListener(ConfigurationListener configurationListener){
            listeners.remove(configurationListener);
        }
    }
}
