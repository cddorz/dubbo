package org.apache.dubbo.remoting.etcd.jetcd;

import io.etcd.jetcd.Client;

public interface ConnectionStateListener {


    void stateChanged(Client client, int newState);
}
