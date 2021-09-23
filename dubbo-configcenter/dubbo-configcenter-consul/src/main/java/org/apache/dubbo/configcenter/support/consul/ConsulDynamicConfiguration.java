package org.apache.dubbo.configcenter.support.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.TreePathDynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.common.utils.StringUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.dubbo.common.constants.CommonConstants.DOT_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-12 20:02
 */
public class ConsulDynamicConfiguration extends TreePathDynamicConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsulDynamicConfiguration.class);

    private ConsulClient client;

    private String rootPath;

    private final Set<String> services = new ConcurrentHashSet<>();

    private ExecutorService notifierExecutor = Executors.newCachedThreadPool(
        new NamedThreadFactory("dubbo-service-discovery-consul-notifier", true)
    );
    private Map<String, ConsulNotifier> notifiers = new ConcurrentHashMap<>();

    private Map<String, ConfigurationDataListener> casListenerMap = new ConcurrentHashMap<>();

    private static final String WATCH_TIMEOUT = "consul-watch-timeout";

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;
    public ConsulDynamicConfiguration(URL url) {
        super(url);
        String host = url.getHost();
        int port = INVALID_PORT != url.getPort() ? url.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
        rootPath = getRootPath(url);
    }

    @Override
    public String getInternalProperty(String key){
        try {
            Response<GetValue> value = client.getKVValue(buildPathKey("",key));
            if (value != null && value.getValue() != null) {
                return value.getValue().getDecodedValue();
            }
            return null;
        } catch (Throwable t) {
            logger.error("Failed to get " + key + " from consul , cause: " + t.getMessage(), t);
            throw new RuntimeException("Failed to get " + key + " from consul , cause: " + t.getMessage(), t);
        }
    }

    @Override
    protected void doClose() throws Exception {
        notifierExecutor.shutdown();
    }


    @Override
    protected boolean doPublishConfig(String pathKey, String content){
        try {
            client.setKVValue(pathKey,content);
            services.add(pathKey);
            return true;
        } catch (Throwable t) {
            logger.error("Failed to put " + pathKey + " to consul " + content + ", cause: " + t.getMessage(), t);
            throw new RuntimeException("Failed to put " + pathKey + " to consul " + content + ", cause: " + t.getMessage(), t);
        }
    }

    @Override
    protected String doGetConfig(String pathKey) throws Exception {
        Response<GetValue> value = client.getKVValue(pathKey);
        if (value != null && value.getValue() != null) {
            return value.getValue().getDecodedValue();
        }
        return null;
    }

    @Override
    protected boolean doRemoveConfig(String pathKey)  {
        try {
            client.deleteKVValue(pathKey);
            return true;
        }catch (Throwable r){
            logger.error("Failed to delete " + pathKey + ", cause: " + r.getMessage(),r);
            throw new RuntimeException("Failed to delete " + pathKey + ", cause: " + r.getMessage(),r);
        }
    }

    @Override
    public boolean publishConfigCas(String key, String group, String content, Object ticket) {
        String path = buildPathKey(group,key);
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
    protected Collection<String> doGetConfigKeys(String groupPath) {
        return services;
    }

    @Override
    public ConfigItem getConfigItem(String key, String group) {
        String path = buildPathKey(group,key);
        Response<GetValue> kvValue = client.getKVValue(path);
        String content = kvValue.getValue().getDecodedValue();
        return new ConfigItem(content,content);
    }


    @Override
    protected void doAddListener(String pathKey, ConfigurationListener listener) {
        ConfigurationDataListener configurationDataLitsener = casListenerMap.computeIfAbsent(pathKey,k->new ConfigurationDataListener(pathKey));
        configurationDataLitsener.addListener(listener);
        ConsulNotifier notifier = notifiers.computeIfAbsent(pathKey,k -> new ConsulNotifier(pathKey));
        notifierExecutor.submit(notifier);
    }

    @Override
    protected void doRemoveListener(String pathKey, ConfigurationListener listener) {
        casListenerMap.remove(pathKey);
        ConsulNotifier notifier = notifiers.get(pathKey);
        notifier.stop();
        notifiers.remove(pathKey);
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

    private class ConfigurationDataListener {

        private String serviceKey;
        private Set<ConfigurationListener> listeners;

        public ConfigurationDataListener(String serviceKey) {
            this.serviceKey = serviceKey;
            this.listeners = new HashSet<>();
        }

        public void addListener(ConfigurationListener listener) {
            this.listeners.add(listener);
        }
    }

    private class ConsulNotifier implements  Runnable{
        private boolean running;
        String path;
        private final ConfigurationDataListener configurationDataLisentner;

        public ConsulNotifier(String path){
            this.path = path;
            this.running = true;
            this.configurationDataLisentner = casListenerMap.computeIfAbsent(path, k -> new ConfigurationDataListener(path));
        }
        @Override
        public void run() {
            while (this.running){
                String key = pathToKey(path);
                try {
                    ConfigChangedEvent event = new ConfigChangedEvent(key, getGroup(path), doGetConfig(path));
                    configurationDataLisentner.listeners.forEach(listener -> listener.process(event));
                } catch (Throwable r) {
                    throw new RuntimeException(r.getMessage(),r);
                }
            }
        }

        public void stop(){
            this.running = false;
        }
    }
}
