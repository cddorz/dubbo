package org.apache.dubbo.configcenter.support.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.AbstractDynamicConfigurationFactory;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-22 20:27
 */
public class EtcdDynamicConfigurationFactory extends AbstractDynamicConfigurationFactory {
    @Override
    protected DynamicConfiguration createDynamicConfiguration(URL url) {
        return new EtcdDynamicConfiguration(url);
    }
}
