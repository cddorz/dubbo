package org.apache.dubbo.remoting.etcd;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:20
 */

public interface RetryPolicy {


    boolean shouldRetry(int retried, long elapsed, boolean sleep);

}
