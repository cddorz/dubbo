package org.apache.dubbo.configcenter.support.redis;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.TreePathDynamicConfiguration;

import java.util.Collection;



public class RedisDynamicConfiguration extends TreePathDynamicConfiguration {
    public RedisDynamicConfiguration(URL url) {
        super(url);
    }

    @Override
    protected void doClose() throws Exception {

    }

    @Override
    protected boolean doPublishConfig(String pathKey, String content) throws Exception {
        return false;
    }

    @Override
    protected String doGetConfig(String pathKey) throws Exception {
        return null;
    }

    @Override
    protected boolean doRemoveConfig(String pathKey) throws Exception {
        return false;
    }

    @Override
    protected Collection<String> doGetConfigKeys(String groupPath) {
        return null;
    }

    @Override
    protected void doAddListener(String pathKey, ConfigurationListener listener) {

    }

    @Override
    protected void doRemoveListener(String pathKey, ConfigurationListener listener) {

    }
}
