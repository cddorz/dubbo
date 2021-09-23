package org.apache.dubbo.remoting.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.remoting.Constants;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-17 19:20
 */
@SPI("jetcd")
public interface EtcdTransporter {

    @Adaptive({Constants.CLIENT_KEY, Constants.TRANSPORTER_KEY})
    EtcdClient connect(URL url);

}
