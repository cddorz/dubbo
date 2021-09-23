package org.apache.dubbo.remoting.etcd;


import org.apache.dubbo.common.URL;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:20
 */
public interface EtcdClient {

    /**
     * save the specified path to the etcd registry.
     *
     * @param path the path to be saved
     */
    void create(String path);

    /**
     * save the specified path to the etcd registry.
     * if node disconnect from etcd, it will be deleted
     * automatically by etcd when session timeout.
     *
     * @param path the path to be saved
     * @return the lease of current path.
     */
    long createEphemeral(String path);

    /**
     * remove the specified  from etcd registry.
     *
     * @param path the path to be removed
     */
    void delete(String path);

    /**
     * find direct children directory, excluding path self,
     * Never return null.
     *
     * @param path the path to be found direct children.
     * @return direct children directory, contains zero element
     * list if children directory not exists.
     */
    List<String> getChildren(String path);

    /**
     * register children listener for specified path.
     *
     * @param path     the path to be watched when children is added, delete or update.
     * @param listener when children is changed , listener will be triggered.
     * @return direct children directory, contains zero element
     * list if children directory not exists.
     */
    List<String> addChildListener(String path, ChildListener listener);

    /**
     * find watcher of the children listener for specified path.
     *
     * @param path     the path to be watched when children is added, delete or update.
     * @param listener when children is changed , listener will be triggered.
     * @return watcher if find else null
     */
    <T> T getChildListener(String path, ChildListener listener);

    /**
     * unregister children lister for specified path.
     *
     * @param path     the path to be unwatched .
     * @param listener when children is changed , lister will be triggered.
     */
    void removeChildListener(String path, ChildListener listener);

    /**
     * support connection notify if connection state was changed.
     *
     * @param listener if state changed, listener will be triggered.
     */
    void addStateListener(StateListener listener);

    /**
     * remove connection notify if connection state was changed.
     *
     * @param listener remove already registered listener, if listener
     *                 not exists nothing happened.
     */
    void removeStateListener(StateListener listener);

    /**
     * test if current client is active.
     *
     * @return true if connection is active else false.
     */
    boolean isConnected();

    /**
     * close current client and release all resourses.
     */
    void close();

    URL getUrl();

    /***
     * create new lease from specified second ,it should be waiting if failed.<p>
     *
     * @param second lease time (support second only).
     * @return lease id from etcd
     */
    long createLease(long second);

    /***
     * create new lease from specified ttl second before waiting specified timeout.<p>
     *
     * @param ttl lease time (support second only).
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return lease id from etcd
     */
    long createLease(long ttl, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * revoke specified lease, any associated path will removed automatically.
     *
     * @param lease to be removed lease
     */
    void revokeLease(long lease);


    /**
     * Get the value of the specified key.
     * @param key the specified key
     * @return null if the value is not found
     */
    String getKVValue(String key);

    /**
     * Put the key value pair to etcd
     * @param key the specified key
     * @param value the paired value
     * @return true if put success
     */
    boolean put(String key, String value);

    /**
     * Put the key value pair to etcd (Ephemeral)
     * @param key the specified key
     * @param value the paired value
     * @return true if put success
     */
    boolean putEphemeral(String key, String value);

}
