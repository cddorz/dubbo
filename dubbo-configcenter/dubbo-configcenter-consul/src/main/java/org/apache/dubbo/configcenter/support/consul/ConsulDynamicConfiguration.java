package org.apache.dubbo.configcenter.support.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.TreePathDynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.NamedThreadFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-12 20:02
 */
public class ConsulDynamicConfiguration extends TreePathDynamicConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ConsulDynamicConfiguration.class);

    private ConsulClient client;

    private final Set<String> services = new ConcurrentHashSet<>();

    private ExecutorService notifierExecutor = Executors.newCachedThreadPool(
        new NamedThreadFactory("dubbo-service-discovery-consul-notifier", true)
    );
    private Map<String, ConsulNotifier> notifiers = new ConcurrentHashMap<>();

    private static final int DEFAULT_WATCH_TIMEOUT = 60 * 1000;

    private static final String WATCH_TIMEOUT = "consul-watch-timeout";

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;
    public ConsulDynamicConfiguration(URL url) {
        super(url);
        String host = url.getHost();
        int port = INVALID_PORT != url.getPort() ? url.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
    }

    @Override
    public String getInternalProperty(String key){
        try {
            Response<GetValue> value = client.getKVValue(key);
            if (value != null && value.getValue() != null) {
                return value.getValue().getValue();
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
        return getInternalProperty(pathKey);
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
    protected Collection<String> doGetConfigKeys(String groupPath) {
        return services;
    }


    @Override
    protected void doAddListener(String pathKey, ConfigurationListener listener) {
        ConsulNotifier notifier = notifiers.computeIfAbsent(pathKey,k -> new ConsulNotifier(pathKey,listener));
        notifierExecutor.submit(notifier);
    }

    @Override
    protected void doRemoveListener(String pathKey, ConfigurationListener listener) {
        notifiers.remove(pathKey);
    }

    private class ConsulNotifier implements Runnable {
        private String pathKey;
        private ConfigurationListener listener;
        private boolean running;

        public ConsulNotifier(String pathKey,ConfigurationListener listener){
            this.listener = listener;
            this.pathKey = pathKey;
            this.running = true;
        }

        @Override
        public void run() {
            while (running){

            }
        }

        public void stop(){
           this.running = false;
        }
    }
}
