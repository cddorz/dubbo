package org.apache.dubbo.remoting.etcd;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:21
 */
public interface StateListener {

    int DISCONNECTED = 0;

    int CONNECTED = 1;

    void stateChanged(int connected);

}
