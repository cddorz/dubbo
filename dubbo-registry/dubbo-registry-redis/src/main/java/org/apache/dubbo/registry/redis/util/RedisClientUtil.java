package org.apache.dubbo.registry.redis.util;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.dubbo.common.URL;

import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.remoting.redis.RedisClient;
import org.apache.dubbo.remoting.redis.jedis.ClusterRedisClient;
import org.apache.dubbo.remoting.redis.jedis.MonoRedisClient;
import org.apache.dubbo.remoting.redis.jedis.SentinelRedisClient;

import static org.apache.dubbo.registry.Constants.REGISTER;
import static org.apache.dubbo.registry.Constants.UNREGISTER;


public class RedisClientUtil {

    private static RedisClient redisClient;

    public static RedisClient buildRedisClient(URL connectedURL) {
        String type = connectedURL.getParameter("redis_client", "mono");
        if ("sentinel".equals(type)) {
            redisClient = new SentinelRedisClient(connectedURL);
        } else if ("cluster".equals(type)) {
            redisClient = new ClusterRedisClient(connectedURL);
        } else {
            redisClient = new MonoRedisClient(connectedURL);
        }
        return redisClient;
    }

    public static void registerServiceInstance(ServiceInstance serviceInstance,int expirePeriod){
        String key = serviceInstance.getServiceName() + "-" +  serviceInstance.getAddress();
        Gson gson = new GsonBuilder().create();
        String value = gson.toJson(serviceInstance);
        String expire = String.valueOf(System.currentTimeMillis() + expirePeriod);
        redisClient.hset(key, value, expire);
        redisClient.publish(key, REGISTER);
    }

    public static void deregisterInstance(ServiceInstance serviceInstance){
        String key = serviceInstance.getServiceName() + "-" +  serviceInstance.getAddress();
        redisClient.hdel(key);
        redisClient.publish(key, UNREGISTER);
    }


}
