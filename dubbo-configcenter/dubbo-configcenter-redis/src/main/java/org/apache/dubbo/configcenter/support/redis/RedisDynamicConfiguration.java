package org.apache.dubbo.configcenter.support.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.rpc.RpcException;
import redis.clients.jedis.*;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.utils.StringUtils.HYPHEN_CHAR;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;


public class RedisDynamicConfiguration implements DynamicConfiguration {

    private static final String PUBLISH = "publish";
    private final static String REDIS_DATABASE_KEY = "database";
    private final static Logger logger = LoggerFactory.getLogger(RedisDynamicConfiguration.class);

    JedisPool pool;
    Set<HostAndPort> jedisClusterNodes;
    private int timeout;
    private String password;

    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<>();

    private Map<String, ConfigurationDataLisentner> casListenerMap = new ConcurrentHashMap<>();

    private static final String luaStr = "local old redis.call(\"\"get\"\", KEYS[1])\n" +
        "if old then\n" +
        "    if old == ARGV[1] then\n" +
        "    redis.call(\"\"set\"\", KEYS[1], ARGV[2])\n" +
        "    return 1\n" +
        "    else\n" +
        "    return 0\n" +
        "    end\n" +
        "else\n" +
        "    redis.call(\"\"set\"\", KEYS[1], ARGV[2])\n" +
        "    return 1\n" +
        "end";


    RedisDynamicConfiguration(URL url){
        timeout = url.getParameter(TIMEOUT_KEY, DEFAULT_TIMEOUT);
        if (url.getParameter(CLUSTER_KEY, false)) {
            jedisClusterNodes = new HashSet<HostAndPort>();
            List<URL> urls = url.getBackupUrls();
            for (URL tmpUrl : urls) {
                jedisClusterNodes.add(new HostAndPort(tmpUrl.getHost(), tmpUrl.getPort()));
            }
        } else {
            int database = url.getParameter(REDIS_DATABASE_KEY, 0);
            pool = new JedisPool(new JedisPoolConfig(), url.getHost(), url.getPort(), timeout, url.getPassword(), database);
        }
    }

    @Override
    public Object getInternalProperty(String key) {
        String keyPath = buildKey(key,DEFAULT_GROUP);
        return getInProperty(keyPath);
    }

    @Override
    public void addListener(String key, String group, ConfigurationListener listener) {
        String listenerKey = buildKey(key,group);
        ConfigurationDataLisentner configurationDataLisentner = casListenerMap.computeIfAbsent(listenerKey,k->new ConfigurationDataLisentner(listenerKey));
        configurationDataLisentner.addListener(listener);
        Notifier notifier = notifiers.get(listenerKey);
        if(notifier == null){
            Notifier newnotifier = new Notifier(listenerKey);
            notifiers.putIfAbsent(listenerKey,newnotifier);
            notifier = notifiers.get(listenerKey);
            if(notifier == newnotifier){
                notifier.start();
            }
        }

    }

    @Override
    public void removeListener(String key, String group, ConfigurationListener listener) {
        String listenerKey = buildKey(key,group);
        ConfigurationDataLisentner configurationDataLisentner = casListenerMap.computeIfAbsent(listenerKey,k->new ConfigurationDataLisentner(listenerKey));
        configurationDataLisentner.removeListener(listener);
        Notifier notifier = notifiers.get(listenerKey);
        notifier.shutdown();
    }

    @Override
    public ConfigItem getConfigItem(String key, String group){
        String keyPath = buildKey(key,group);
        String content = getInProperty(keyPath);
        return new ConfigItem(content,content);
    }

    @Override
    public String getConfig(String key, String group, long timeout) throws IllegalStateException {
        return null;
    }

    @Override
    public boolean publishConfigCas(String key, String group, String content, Object ticket) {
        try {
            String keyPath = buildKey(key,group);
            return registerConfigCas(keyPath,content,ticket);
        }catch (Throwable t){
            logger.warn("Failed to publish config from redis, cause: " + t.getMessage(),t);
            throw new RpcException("Failed to publish config from redis, cause: " + t.getMessage(),t);
        }
    }

    @Override
    public boolean removeConfig(String key, String group) {
        String keyPath = buildKey(key,group);
        return removedConfig(keyPath);
    }

    private boolean registerConfigCas(String key,String content,Object ticket){
        if(pool != null){
            return registerConfigCasStandalone(key,content,ticket);
        }else {
            return registerConfigCasInCluster(key,content,ticket);
        }
    }

    private boolean registerConfigCasStandalone(String key,String content,Object ticket){
        try (Jedis jedis = pool.getResource()){
            jedis.publish(key,PUBLISH);
            if(ticket != null){
                String ticketToString = ticket.toString();
                List<String> keys = new ArrayList<String>();
                keys.add(key);
                List<String> values = new ArrayList<String>();
                values.add(ticketToString);
                String newValue = ticketToString + "," + content;
                values.add(newValue);
                Object result = jedis.eval(luaStr, keys,values);
                Long tag = (Long) result;
                return tag != 0;
            }else{
                String ticketToString = "";
                List<String> keys = new ArrayList<String>();
                keys.add(key);
                List<String> values = new ArrayList<String>();
                values.add(ticketToString);
                String newValue = content;
                values.add(newValue);
                Object result = jedis.eval(luaStr, keys, values);
                Long tag = (Long) result;
                return tag != 0;
            }
        }catch (Throwable e){
            throw new RpcException("Failed to publish config key:" + key + "from redis, cause: " + e.getMessage() + e);
        }
    }

    private boolean registerConfigCasInCluster(String key,String content,Object ticket){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            jedisCluster.publish(key,PUBLISH);
            if(ticket != null){
                String ticketToString = ticket.toString();
                List<String> keys = new ArrayList<String>();
                keys.add(key);
                List<String> values = new ArrayList<String>();
                values.add(ticketToString);
                String newValue = ticketToString + "," + content;
                values.add(newValue);
                Object result = jedisCluster.eval(luaStr, keys,values);
                Long tag = (Long) result;
                return tag != 0;
            }else {
                String ticketToString = "";
                List<String> keys = new ArrayList<String>();
                keys.add(key);
                List<String> values = new ArrayList<String>();
                values.add(ticketToString);
                String newValue = content;
                values.add(newValue);
                Object result = jedisCluster.eval(luaStr, keys, values);
                Long tag = (Long) result;
                return tag != 0;
            }
        }catch (Throwable e){
            throw new RpcException("Failed to publish config key:" + key +  "from redis, cause: " + e.getMessage() + e);
        }
    }

    private boolean removedConfig(String key){
        if(pool != null){
            return removedConfigStandalone(key);
        }else {
            return removedConfigInCluster(key);
        }
    }

    private boolean removedConfigStandalone(String key){
        try (Jedis jedis = pool.getResource()){
            jedis.del(key);
            return true;
        }catch (Throwable t){
            logger.warn("Failed to remove config from redis, cause: " + t.getMessage(),t);
        }
        return false;
    }

    private boolean removedConfigInCluster(String key){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())){
            jedisCluster.del(key);
            return true;
        }catch (Throwable t){
            logger.warn("Failed to remove config from redis, cause: " + t.getMessage(),t);
        }
        return false;
    }

    private String getInProperty(String key){
        if(pool != null){
            return getInPropertyStandalone(key);
        }else {
            return getInPropertyInCluster(key);
        }
    }

    private String getInPropertyStandalone(String key){
        try (Jedis jedis = pool.getResource()){
            return jedis.get(key);
        }catch (Throwable r){
            logger.warn("Failed to getInternalProperty key: " + key + "from redis, cause: " + r.getMessage(),r);
            throw new RpcException("Failed to getInternalProperty key: " + key + "from redis, cause: " + r.getMessage(), r);
        }
    }

    private String getInPropertyInCluster(String key) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            return jedisCluster.get(key);
        } catch (Throwable r) {
            logger.warn("Failed to getInternalProperty key: " + key + "from redis, cause: " + r.getMessage(), r);
            throw new RpcException("Failed to getInternalProperty key: " + key + "from redis, cause: " + r.getMessage(), r);
        }
    }

    private void subscribe(String key){
        if( pool != null){
            subscribeStandalone(key);
        }else {
            subscribeInCluster(key);
        }
    }

    private void subscribeStandalone(String key){
        try (Jedis jedis = pool.getResource()){
            jedis.subscribe(new NotifySub(key),key);
        }catch (Throwable e){
            logger.error("Failed to subscribe, key: " + key + ", cause: " + e.getMessage(),e);
        }
    }

    private void subscribeInCluster(String key){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            jedisCluster.subscribe(new NotifySub(key),key);
        }catch (Throwable e){
            logger.error("Failed to subscribe, key: " + key + ", cause: " + e.getMessage(),e);
        }
    }

    protected String buildKey(String key, String group) {
        return key + HYPHEN_CHAR + group;
    }

    public static class ConfigurationDataLisentner{

        private Set<ConfigurationListener> listeners;

        private String listenerKey;

        public ConfigurationDataLisentner(String listenerKey){
            this.listeners = new HashSet<>();
        }

        public void addListener(ConfigurationListener listener){
            listeners.add(listener);
        }

        public void removeListener(ConfigurationListener listener){
            listeners.remove(listener);
        }
    }

    private class NotifySub extends JedisPubSub {

        private final ConfigurationDataLisentner configurationDataLisentner;

        public NotifySub(String key){
            this.configurationDataLisentner = casListenerMap.computeIfAbsent(key, k -> new ConfigurationDataLisentner(key));
        }

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if(msg.equals(PUBLISH)){
                try {
                    String[] split = key.split("-");
                    String content = getConfig(split[0],split[1]);
                    ConfigChangedEvent event = new ConfigChangedEvent(split[0],split[1],content);
                    configurationDataLisentner.listeners.forEach(listener -> listener.process(event));
                }catch (Throwable r){
                    logger.warn(r.getMessage(),r);
                }
            }
        }
        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }
    }
    private class Notifier extends Thread{

        private String key;

        private volatile boolean running = true;

        public Notifier(String key) {
            super.setDaemon(true);
            super.setName("DubboRedisConfigSubscribe");
            this.key = key;
        }

        @Override
        public void run(){
            while (running){
                try {
                    subscribe(key);
                }catch (Throwable e){
                    logger.warn(e.getMessage(),e);
                }
            }
        }

        public void shutdown(){
            running = false;
        }
    }
}
