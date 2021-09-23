package org.apache.dubbo.remoting.etcd.jetcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.etcd.EtcdClient;
import org.apache.dubbo.remoting.etcd.EtcdTransporter;

public class JEtcdTransporter implements EtcdTransporter {

    @Override
    public EtcdClient connect(URL url) {
        return new JEtcdClient(url);
    }

}
