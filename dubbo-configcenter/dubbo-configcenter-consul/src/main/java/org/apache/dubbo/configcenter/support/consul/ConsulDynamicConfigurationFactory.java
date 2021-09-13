package org.apache.dubbo.configcenter.support.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.AbstractDynamicConfigurationFactory;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-12 20:02
 */
public class ConsulDynamicConfigurationFactory extends AbstractDynamicConfigurationFactory {
    @Override
    protected DynamicConfiguration createDynamicConfiguration(URL url) {
        return new ConsulDynamicConfiguration(url);
    }
}
