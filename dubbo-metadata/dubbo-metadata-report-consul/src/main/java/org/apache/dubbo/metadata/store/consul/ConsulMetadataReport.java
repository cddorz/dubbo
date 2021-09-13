package org.apache.dubbo.metadata.store.consul;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.report.identifier.*;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-09 20:59
 */
public class ConsulMetadataReport extends AbstractMetadataReport {

    private final static Logger logger = LoggerFactory.getLogger(ConsulMetadataReport.class);

    private static final int DEFAULT_PORT = 8500;

    private static final int INVALID_PORT = 0;

    private ConsulClient client;


    public ConsulMetadataReport(URL registryURL){
        super(registryURL);
        String host = registryURL.getHost();
        int port = INVALID_PORT != registryURL.getPort() ? registryURL.getPort() : DEFAULT_PORT;
        client = new ConsulClient(host, port);
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        this.storeMetadata(providerMetadataIdentifier, serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
        this.storeMetadata(consumerMetadataIdentifier, serviceParameterString);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        this.storeMetadata(metadataIdentifier, URL.encode(url.toFullString()));
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        this.deleteMetadata(metadataIdentifier);
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        String content = getMetadata(metadataIdentifier);
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<String>(Arrays.asList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        this.storeMetadata(subscriberMetadataIdentifier, urlListStr);
    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        return getMetadata(subscriberMetadataIdentifier);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        return getMetadata(metadataIdentifier);
    }

    private void storeMetadata(BaseMetadataIdentifier identifier, String v) {
        try {
            client.setKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY), v);
        } catch (Throwable t) {
            logger.error("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
            throw new RpcException("Failed to put " + identifier + " to consul " + v + ", cause: " + t.getMessage(), t);
        }
    }

    private void deleteMetadata(BaseMetadataIdentifier identifier) {
        try {
            client.deleteKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
        } catch (Throwable t) {
            logger.error("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to delete " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }

    private String getMetadata(BaseMetadataIdentifier identifier) {
        try {
            Response<GetValue> value = client.getKVValue(identifier.getUniqueKey(KeyTypeEnum.UNIQUE_KEY));
            if (value != null && value.getValue() != null) {
                return value.getValue().getValue();
            }
            return null;
        } catch (Throwable t) {
            logger.error("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
            throw new RpcException("Failed to get " + identifier + " from consul , cause: " + t.getMessage(), t);
        }
    }
}
