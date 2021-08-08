package org.apache.dubbo.registry.redis;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.client.AbstractServiceDiscovery;

import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.ServiceInstancesChangedEvent;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.remoting.redis.RedisClient;
import org.apache.dubbo.rpc.RpcException;
import redis.clients.jedis.JedisPubSub;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.dubbo.registry.Constants.*;
import static org.apache.dubbo.registry.Constants.DEFAULT_REGISTRY_RECONNECT_PERIOD;
import static org.apache.dubbo.registry.redis.util.RedisClientUtil.buildRedisClient;
import static org.apache.dubbo.registry.redis.util.RedisClientUtil.registerServiceInstance;
import static org.apache.dubbo.registry.redis.util.RedisClientUtil.deregisterInstance;

public class RedisServiceDiscovery extends AbstractServiceDiscovery {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final ConcurrentMap<String, Notifier> notifiers = new ConcurrentHashMap<>();

    private final ScheduledExecutorService expireExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("DubboRegistryExpireTimer", true));

    private ScheduledFuture<?> expireFuture;

    private URL registryURL;

    private RedisClient redisClient;

    private int expirePeriod;

    private int reconnectPeriod;

    private volatile boolean admin = false;

    private ServiceInstancesChangedListener listener;

    @Override
    public void doInitialize(URL registryURL) throws Exception {
        this.registryURL = registryURL;
        this.redisClient = buildRedisClient(registryURL);
        this.expirePeriod = registryURL.getParameter(SESSION_TIMEOUT_KEY, DEFAULT_SESSION_TIMEOUT);
        this.reconnectPeriod = registryURL.getParameter(REGISTRY_RECONNECT_PERIOD_KEY, DEFAULT_REGISTRY_RECONNECT_PERIOD);
        this.expireFuture = expireExecutor.scheduleWithFixedDelay(() -> {
            try {
                deferExpired(); // Extend the expiration time
            } catch (Throwable t) { // Defensive fault tolerance
                logger.error("Unexpected exception occur at defer expire time, cause: " + t.getMessage(), t);
            }
        }, expirePeriod / 2, expirePeriod / 2, TimeUnit.MILLISECONDS);
    }
    private void deferExpired() {
        for(String serviceName : getServices()){
            for(ServiceInstance serviceInstance : getInstances(serviceName)){
                registerServiceInstance(serviceInstance,expirePeriod);
            }
        }
        if (admin) {
            clean();
        }
    }

    private void clean() {
        Set<String> keys = getServices();
        if (CollectionUtils.isNotEmpty(keys)) {
            for (String key : keys) {
                Map<String, String> values = redisClient.hgetAll(key);
                if (CollectionUtils.isNotEmptyMap(values)) {
                    boolean delete = false;
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, String> entry : values.entrySet()) {
                        long expire = Long.parseLong(entry.getValue());
                        if (expire < now) {
                            redisClient.hdel(key, entry.getKey());
                            delete = true;
                            if (logger.isWarnEnabled()) {
                                logger.warn("Delete expired key: " + key + " -> value: " + entry.getKey() + ", expire: " + new Date(expire) + ", now: " + new Date(now));
                            }
                        }
                    }
                    if (delete) {
                        redisClient.publish(key, UNREGISTER);
                    }
                }
            }
        }
    }

    @Override
    public void doRegister(ServiceInstance serviceInstance){
        try{
            registerServiceInstance(serviceInstance,expirePeriod);
        }catch (Exception e){
            throw new RpcException("Failed register instance " + serviceInstance.toString(), e);
        }
    }

    @Override
    public void doUpdate(ServiceInstance serviceInstance) throws RuntimeException {
            ServiceInstance oldServiceInstance = this.serviceInstance;
            this.unregister(oldServiceInstance);
            this.register(serviceInstance);
    }

    @Override
    public void doUnregister(ServiceInstance serviceInstance) {
        try {
            deregisterInstance(serviceInstance);
        }catch (Exception e){
            throw new RpcException("Failed unregister instance" + serviceInstance.toString(),e);
        }
    }

    @Override
    public void doDestroy() throws Exception {
        try {
            redisClient.destroy();
        }catch (Throwable t){
            logger.warn(t.getMessage(),t);
        }
        try {
            for (Notifier notifier : notifiers.values()) {
                notifier.shutdown();
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        try {
            expireFuture.cancel(true);
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
        ExecutorUtil.gracefulShutdown(expireExecutor, expirePeriod);
    }

    @Override
    public Set<String> getServices() {
        return redisClient.keys();
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName){
        Gson gson = new GsonBuilder().create();
        Map<String, String> values = redisClient.hgetAll(serviceName);
        List<ServiceInstance> serviceInstances = new ArrayList<>();
        Set<String> serviceInstanceSet = values.keySet();
        Iterator<String> it =serviceInstanceSet.iterator();
        while (it.hasNext()){
            String serviceInstanceToString = it.next();
            ServiceInstance serviceInstance = gson.fromJson(serviceInstanceToString,ServiceInstance.class);
            serviceInstances.add(serviceInstance);
        }
        return serviceInstances;
    }

    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener)
        throws NullPointerException, IllegalArgumentException{
        this.listener = listener;
        listener.getServiceNames().forEach(serviceName -> registerServiceWatcher(serviceName, listener));
    }

    protected void registerServiceWatcher(String serviceName, ServiceInstancesChangedListener listener){
        Notifier notifier = notifiers.get(serviceName);
        if(notifier == null){
            Notifier newnotifier = new Notifier(serviceName);
            notifiers.putIfAbsent(serviceName,newnotifier);
            notifier = notifiers.get(serviceName);
            if(notifier == newnotifier){
                notifier.start();
            }
        }
        admin = true;
        listener.onEvent(new ServiceInstancesChangedEvent(serviceName,this.getInstances(serviceName)));
    }

    private class NotifySub extends JedisPubSub{

        public NotifySub(){}

        @Override
        public void onMessage(String key, String msg) {
            if (logger.isInfoEnabled()) {
                logger.info("redis event: " + key + " = " + msg);
            }
            if (msg.equals(REGISTER)
                || msg.equals(UNREGISTER)) {
                try {
                    listener.onEvent(new ServiceInstancesChangedEvent(key,getInstances(key)));
                }catch (Throwable t){
                    logger.warn(t.getMessage(),t);
                }
            }
        }
        @Override
        public void onPMessage(String pattern, String key, String msg) {
            onMessage(key, msg);
        }

        @Override
        public void onSubscribe(String key, int num) {
        }

        @Override
        public void onPSubscribe(String pattern, int num) {
        }

        @Override
        public void onUnsubscribe(String key, int num) {
        }

        @Override
        public void onPUnsubscribe(String pattern, int num) {
        }

    }

    private class Notifier extends Thread {
        private final String serviceName;
        private final AtomicInteger connectSkip = new AtomicInteger();
        private final AtomicInteger connectSkipped = new AtomicInteger();

        private volatile boolean first = true;
        private volatile boolean running = true;
        private volatile int connectRandom;

        public Notifier(String serviceName) {
            super.setDaemon(true);
            super.setName("DubboRedisSubscribe");
            this.serviceName = serviceName;
        }

        private void resetSkip() {
            connectSkip.set(0);
            connectSkipped.set(0);
            connectRandom = 0;
        }

        private boolean isSkip() {
            int skip = connectSkip.get(); // Growth of skipping times
            if (skip >= 10) { // If the number of skipping times increases by more than 10, take the random number
                if (connectRandom == 0) {
                    connectRandom = ThreadLocalRandom.current().nextInt(10);
                }
                skip = 10 + connectRandom;
            }
            if (connectSkipped.getAndIncrement() < skip) { // Check the number of skipping times
                return true;
            }
            connectSkip.incrementAndGet();
            connectSkipped.set(0);
            connectRandom = 0;
            return false;
        }

        @Override
        public void run(){
            while (running){
                try {
                    if(!isSkip()){
                        try {
                            if (!redisClient.isConnected()) {
                                continue;
                            }
                            try {
                                redisClient.psubscribe(new NotifySub(),serviceName);
                            }catch (Throwable t){
                                logger.warn("Failed to subscribe service from redis registry. cause:" + t.getMessage(),t);
                                sleep(reconnectPeriod);
                            }
                        }catch (Throwable t){
                            logger.error(t.getMessage(),t);
                            sleep(reconnectPeriod);
                        }
                    }
                }catch (Throwable t){
                    logger.error(t.getMessage(),t);
                }
            }
    }

        public void shutdown() {
            try {
                running = false;
                redisClient.disconnect();
            } catch (Throwable t) {
                logger.warn(t.getMessage(), t);
            }
        }
    }
}
