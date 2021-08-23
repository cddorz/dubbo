/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.metadata.store.redis;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.report.identifier.BaseMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.KeyTypeEnum;
import org.apache.dubbo.metadata.report.identifier.MetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.ServiceMetadataIdentifier;
import org.apache.dubbo.metadata.report.identifier.SubscriberMetadataIdentifier;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.utils.StringUtils.HYPHEN_CHAR;
import static org.apache.dubbo.metadata.MetadataConstants.META_DATA_STORE_TAG;
import static org.apache.dubbo.metadata.ServiceNameMapping.DEFAULT_MAPPING_GROUP;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;

/**
 * RedisMetadataReport
 */
public class RedisMetadataReport extends AbstractMetadataReport {

    private final static String REDIS_DATABASE_KEY = "database";
    private final static Logger logger = LoggerFactory.getLogger(RedisMetadataReport.class);

    JedisPool pool;
    Set<HostAndPort> jedisClusterNodes;
    private int timeout;
    private String password;
    private final static String SERVICE_APP_MAPPING = "mapping";

    private Map<String, MappingDataListener> casListenerMap = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<>();

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


    public RedisMetadataReport(URL url) {
        super(url);
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
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        this.storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String value) {
        this.storeMetadata(consumerMetadataIdentifier, value);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier, URL url) {
        this.storeMetadata(serviceMetadataIdentifier, URL.encode(url.toFullString()));
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier serviceMetadataIdentifier) {
        this.deleteMetadata(serviceMetadataIdentifier);
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
        return this.getMetadata(subscriberMetadataIdentifier);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return this.getMetadata(metadataIdentifier);
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, URL url){
        String key = buildKey(DEFAULT_MAPPING_GROUP, serviceKey);
        return getAppNames(getServiceAppNames(key));
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        String key = buildKey(DEFAULT_MAPPING_GROUP,serviceKey);
        if( null == casListenerMap.get(key)){
            addCasServiceMappingListener(key,listener);
        }
        return getAppNames(getServiceAppNames(key));
    }

    @Override
    public boolean registerServiceAppMapping(String key, String group, String content, Object ticket){
        try {
            String keyPath = buildKey(group,key);
            return doRegisterServiceAppMapping(keyPath,content, ticket);
        }catch (Exception e){
            logger.warn("redis publishConfigCas failed.", e);
            return false;
        }
    }

    @Override
    public ConfigItem getConfigItem(String key, String group) {
        String pathKey = buildKey(group,key);
        String content = getConfig(pathKey);
        return new ConfigItem(content,content);
    }

    public void addCasServiceMappingListener(String key,MappingListener listener){
        MappingDataListener mappingDataListener = casListenerMap.computeIfAbsent(key, k -> new MappingDataListener(key));
        mappingDataListener.addListener(listener);
        Notifier notifier = notifiers.get(key);
        if(notifier == null){
            Notifier newnotifier = new Notifier(key);
            notifiers.putIfAbsent(key,newnotifier);
            notifier = notifiers.get(key);
            if(notifier == newnotifier){
                notifier.start();
            }
        }
    }

    private String getConfig(String key){
        if (pool != null) {
            return getConfigStandalone(key);
        }else {
            return getConfigInCluster(key);
        }
    }

    private String getConfigStandalone(String key){
        try (Jedis jedis = pool.getResource()) {
            return jedis.get(key);
        }catch (Throwable e){
            logger.error("Failed to get content from redis, cause: " + e.getMessage(),e);
            throw new RpcException("Failed to get content from redis, cause: " + e.getMessage(),e);
        }
    }

    private String getConfigInCluster(String key){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            return jedisCluster.get(key);
        } catch (Throwable e) {
            logger.error("Failed to get content from redis cluster, cause: " + e.getMessage(),e);
            throw new RpcException("Failed to get content from redis cluster, cause: " + e.getMessage(),e);
        }
    }

    private void storeMetadata(BaseMetadataIdentifier metadataIdentifier, String v) {
        if (pool != null) {
            storeMetadataStandalone(metadataIdentifier, v);
        } else {
            storeMetadataInCluster(metadataIdentifier, v);
        }
    }

    private void storeMetadataInCluster(BaseMetadataIdentifier metadataIdentifier, String v) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            jedisCluster.set(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG, v);
        } catch (Throwable e) {
            logger.error("Failed to put " + metadataIdentifier + " to redis cluster " + v + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + metadataIdentifier + " to redis cluster " + v + ", cause: " + e.getMessage(), e);
        }
    }

    private void storeMetadataStandalone(BaseMetadataIdentifier metadataIdentifier, String v) {
        try (Jedis jedis = pool.getResource()) {
            jedis.set(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), v);
        } catch (Throwable e) {
            logger.error("Failed to put " + metadataIdentifier + " to redis " + v + ", cause: " + e.getMessage(), e);
            throw new RpcException("Failed to put " + metadataIdentifier + " to redis " + v + ", cause: " + e.getMessage(), e);
        }
    }

    private void deleteMetadata(BaseMetadataIdentifier metadataIdentifier) {
        if (pool != null) {
            deleteMetadataStandalone(metadataIdentifier);
        } else {
            deleteMetadataInCluster(metadataIdentifier);
        }
    }

    private void deleteMetadataInCluster(BaseMetadataIdentifier metadataIdentifier) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            jedisCluster.del(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG);
        } catch (Throwable e) {
            logger.error("Failed to delete " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to delete " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
        }
    }

    private void deleteMetadataStandalone(BaseMetadataIdentifier metadataIdentifier) {
        try (Jedis jedis = pool.getResource()) {
            jedis.del(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable e) {
            logger.error("Failed to delete " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to delete " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
        }
    }

    private String getMetadata(BaseMetadataIdentifier metadataIdentifier) {
        if (pool != null) {
            return getMetadataStandalone(metadataIdentifier);
        } else {
            return getMetadataInCluster(metadataIdentifier);
        }
    }

    private String getMetadataInCluster(BaseMetadataIdentifier metadataIdentifier) {
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            return jedisCluster.get(metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG);
        } catch (Throwable e) {
            logger.error("Failed to get " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to get " + metadataIdentifier + " from redis cluster , cause: " + e.getMessage(), e);
        }
    }

    private String getMetadataStandalone(BaseMetadataIdentifier metadataIdentifier) {
        try (Jedis jedis = pool.getResource()) {
            return jedis.get(metadataIdentifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable e) {
            logger.error("Failed to get " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
            throw new RpcException("Failed to get " + metadataIdentifier + " from redis , cause: " + e.getMessage(), e);
        }
    }

    private boolean doRegisterServiceAppMapping(String key, String content,Object ticket){
        if(pool != null){
            return registerServiceAppMappingStandalone(key,content,ticket);
        }else {
            return registerServiceAppMappingInCluster(key,content,ticket);
        }
    }

    private boolean registerServiceAppMappingInCluster(String key,  String content, Object ticket){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            jedisCluster.publish(key,SERVICE_APP_MAPPING);
            String ticketToString = ticket.toString();
            List<String> keys = new ArrayList<String>();
            keys.add(key);
            List<String> values = new ArrayList<String>();
            values.add(ticketToString);
            String newValue = ticketToString + "," + content;
            values.add(newValue);
            Object result = jedisCluster.eval(luaStr, keys,values);
            int tag = (int) result;
            return tag != 0;
        }catch (Throwable e){
            throw new RpcException("Failed to register serviceAppMapping key:" + key +  "from redis, cause: " + e.getMessage() + e);
        }
    }

    private boolean registerServiceAppMappingStandalone(String key,  String content,Object ticket){
        try (Jedis jedis = pool.getResource()){
            jedis.publish(key,SERVICE_APP_MAPPING);
            String ticketToString = ticket.toString();
            List<String> keys = new ArrayList<String>();
            keys.add(key);
            List<String> values = new ArrayList<String>();
            values.add(ticketToString);
            String newValue = ticketToString + "," + content;
            values.add(newValue);
            Object result = jedis.eval(luaStr, keys,values);
            int tag = (int) result;
            return tag != 0;
        }catch (Throwable e){
            throw new RpcException("Failed to register serviceAppMapping key:" + key + "from redis, cause: " + e.getMessage() + e);
        }
    }

    private String getServiceAppNames(String key){
        if (pool != null){
            return getAppNamesStandalone(key);
        }else {
            return getAppNamesInCluster(key);
        }
    }

    private String getAppNamesInCluster(String key){
        try (JedisCluster jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout, 2, password, new GenericObjectPoolConfig())) {
            return jedisCluster.get(key);
        }catch (Throwable e){
            throw new RpcException("Failed to get key:" + key + "from redis, cause: " + e.getMessage() + e);
        }
    }

    private String getAppNamesStandalone(String key){
        try (Jedis jedis = pool.getResource()){
            return jedis.get(key);
        }catch (Throwable e){
            throw new RpcException("Failed to get key:" + key + "from redis, cause: " + e.getMessage() + e);
        }
    }

    private String buildKey(String group,String serviceKey){
        return group + HYPHEN_CHAR + serviceKey;
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

    private class NotifySub extends JedisPubSub {

        private final MappingDataListener mappingDataListener;

        public NotifySub(String key){
            this.mappingDataListener = casListenerMap.computeIfAbsent(key, k -> new MappingDataListener(key));
        }

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if(msg.equals(SERVICE_APP_MAPPING)){
                try {
                    Set<String> apps = getAppNames(getServiceAppNames(key));
                    MappingChangedEvent event = new MappingChangedEvent(key,apps);
                    mappingDataListener.listeners.forEach(mappingListener -> mappingListener.onEvent(event));
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

        private final MappingDataListener mappingDataListener;

        private volatile boolean running = true;

        public Notifier(String key) {
              super.setDaemon(true);
              super.setName("DubboRedisMetadataSubscribe");
              this.key = key;
              this.mappingDataListener = casListenerMap.computeIfAbsent(key, k -> new MappingDataListener(key));
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
