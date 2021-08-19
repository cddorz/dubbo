package org.apache.dubbo.metadata.store.apollo;


import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.openapi.client.ApolloOpenApiClient;
import com.ctrip.framework.apollo.openapi.dto.OpenItemDTO;
import com.google.gson.Gson;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.metadata.MappingChangedEvent;
import org.apache.dubbo.metadata.MappingListener;
import org.apache.dubbo.metadata.MetadataInfo;
import org.apache.dubbo.metadata.report.identifier.*;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReport;

import java.security.cert.TrustAnchor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.*;
import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.utils.StringUtils.HYPHEN_CHAR;
import static org.apache.dubbo.metadata.MetadataConstants.META_DATA_STORE_TAG;
import static org.apache.dubbo.metadata.ServiceNameMapping.DEFAULT_MAPPING_GROUP;
import static org.apache.dubbo.metadata.ServiceNameMapping.getAppNames;

public class ApolloMetadataReport extends AbstractMetadataReport {
    private static final Logger logger = LoggerFactory.getLogger(ApolloMetadataReport.class);
    private static final String APOLLO_ENV_KEY = "env";
    private static final String APOLLO_ADDR_KEY = "apollo.meta";
    private static final String APOLLO_CLUSTER_KEY = "apollo.cluster";
    private static final String APOLLO_PROTOCOL_PREFIX = "http://";
    private static final String APOLLO_APPLICATION_KEY = "application";
    private static final String APOLLO_APPID_KEY = "app.id";
    private static final String APOLLO_PORTALURL = "apollo.portalUrl";
    private static final String APOLLO_TOKEN = "apollo.token";
    private static final String APOLLO_USER_ID = "apollo.userId";


    private URL url;
    private Config dubboConfig;
    private ConfigFile dubboConfigFile;
    private ApolloOpenApiClient client;
    private String configEnv = url.getParameter(APOLLO_ENV_KEY);
    private String configAddr = getAddressWithProtocolPrefix(url);
    private String configCluster = url.getParameter(CLUSTER_KEY);
    private String configAppId = url.getParameter(APOLLO_APPID_KEY);
    private String configNamespace = url.getParameter(APOLLO_APPLICATION_KEY);

    private ConcurrentMap<String, ApolloListener> casListeners = new ConcurrentHashMap<>();

    private Gson gson = new Gson();


    public ApolloMetadataReport(URL url) {
        super(url);
        this.url = url;
        // Instead of using Dubbo's configuration, I would suggest use the original configuration method Apollo provides.
        if (StringUtils.isEmpty(System.getProperty(APOLLO_ENV_KEY)) && configEnv != null) {
            System.setProperty(APOLLO_ENV_KEY, configEnv);
        }
        if (StringUtils.isEmpty(System.getProperty(APOLLO_ADDR_KEY)) && !ANYHOST_VALUE.equals(url.getHost())) {
            System.setProperty(APOLLO_ADDR_KEY, configAddr);
        }
        if (StringUtils.isEmpty(System.getProperty(APOLLO_CLUSTER_KEY)) && configCluster != null) {
            System.setProperty(APOLLO_CLUSTER_KEY, configCluster);
        }
        if (StringUtils.isEmpty(System.getProperty(APOLLO_APPID_KEY)) && configAppId != null) {
            System.setProperty(APOLLO_APPID_KEY, configAppId);
        }

        String namespace = url.getParameter(CONFIG_NAMESPACE_KEY, DEFAULT_ROOT);
        String apolloNamespace = StringUtils.isEmpty(namespace) ? url.getGroup(DEFAULT_ROOT) : namespace;
        dubboConfig = ConfigService.getConfig(apolloNamespace);
        dubboConfigFile = ConfigService.getConfigFile(apolloNamespace, ConfigFileFormat.Properties);

        // local protalURL
        String portalUrl = url.getParameter(APOLLO_PORTALURL);
        String token = url.getParameter(APOLLO_TOKEN);
        ApolloOpenApiClient client = ApolloOpenApiClient.newBuilder()
                                                        .withPortalUrl(portalUrl)
                                                        .withToken(token)
                                                        .build();


        // Decide to fail or to continue when failed to connect to remote server.
        boolean check = url.getParameter(CHECK_KEY, true);
        if (dubboConfig.getSourceType() != ConfigSourceType.REMOTE) {
            if (check) {
                throw new IllegalStateException("Failed to connect to config center, the config center is Apollo, " +
                    "the address is: " + (StringUtils.isNotEmpty(configAddr) ? configAddr : configEnv));
            } else {
                logger.warn("Failed to connect to config center, the config center is Apollo, " +
                    "the address is: " + (StringUtils.isNotEmpty(configAddr) ? configAddr : configEnv) +
                    ", will use the local cache value instead before eventually the connection is established.");
            }
        }
    }

    private String getAddressWithProtocolPrefix(URL url) {
        String address = url.getBackupAddress();
        if (StringUtils.isNotEmpty(address)) {
            address = Arrays.stream(COMMA_SPLIT_PATTERN.split(address))
                .map(addr -> {
                    if (addr.startsWith(APOLLO_PROTOCOL_PREFIX)) {
                        return addr;
                    }
                    return APOLLO_PROTOCOL_PREFIX + addr;
                })
                .collect(Collectors.joining(","));
        }
        return address;
    }


    private String buildKey(BaseMetadataIdentifier metadataIdentifier){
        return metadataIdentifier.getIdentifierKey() + META_DATA_STORE_TAG;
    }

    private String buildPath(String group,String key){
        return group + HYPHEN_CHAR + key;
    }

    private void addCasServiceMappingListener(String key, MappingListener listener){
        ApolloListener apolloListener = casListeners.computeIfAbsent(key,k->new ApolloListener());
        apolloListener.addListener(listener);
        dubboConfig.addChangeListener(apolloListener);
    }

    @Override
    public String getServiceDefinition(MetadataIdentifier metadataIdentifier) {
        String key = buildKey(metadataIdentifier);
        return dubboConfig.getProperty(key, null);
    }

    private void storeMetadata(BaseMetadataIdentifier metadataIdentifier,String v){
        OpenItemDTO openItemDTO = new OpenItemDTO();
        openItemDTO.setKey(buildKey(metadataIdentifier));
        openItemDTO.setValue(v);
        openItemDTO.setDataChangeCreatedBy(APOLLO_USER_ID);
        openItemDTO.setComment("store metadata");
        client.createItem(configAppId,configEnv,configCluster,configNamespace,openItemDTO);
    }

    @Override
    protected void doStoreProviderMetadata(MetadataIdentifier providerMetadataIdentifier, String serviceDefinitions) {
        storeMetadata(providerMetadataIdentifier,serviceDefinitions);
    }

    @Override
    protected void doStoreConsumerMetadata(MetadataIdentifier consumerMetadataIdentifier, String serviceParameterString) {
        storeMetadata(consumerMetadataIdentifier,serviceParameterString);
    }

    @Override
    protected void doSaveMetadata(ServiceMetadataIdentifier metadataIdentifier, URL url) {
        OpenItemDTO openItemDTO = new OpenItemDTO();
        openItemDTO.setKey(buildKey(metadataIdentifier));
        openItemDTO.setValue(URL.encode(url.toFullString()));
        openItemDTO.setDataChangeCreatedBy("apollo");
        openItemDTO.setComment("doSaveMetadata");
        client.createItem(configAppId,configEnv,configCluster,configNamespace,openItemDTO);
    }

    @Override
    protected void doRemoveMetadata(ServiceMetadataIdentifier metadataIdentifier) {
        client.removeItem(configAppId,configEnv,configCluster,configNamespace,buildKey(metadataIdentifier),APOLLO_USER_ID);
    }

    @Override
    public void publishAppMetadata(SubscriberMetadataIdentifier identifier, MetadataInfo metadataInfo) {
        String key = buildKey(identifier);
        if(StringUtils.isBlank(dubboConfig.getProperty(key,null))){
            storeMetadata(identifier,gson.toJson(metadataInfo));
        }
    }

    @Override
    public MetadataInfo getAppMetadata(SubscriberMetadataIdentifier identifier, Map<String, String> instanceMetadata) {
        String content = dubboConfig.getProperty(buildKey(identifier),null);
        return gson.fromJson(content, MetadataInfo.class);
    }

    @Override
    protected List<String> doGetExportedURLs(ServiceMetadataIdentifier metadataIdentifier) {
        String key = buildKey(metadataIdentifier);
        String content = dubboConfig.getProperty(key, null);
        if (StringUtils.isEmpty(content)) {
            return Collections.emptyList();
        }
        return new ArrayList<>(Collections.singletonList(URL.decode(content)));
    }

    @Override
    protected void doSaveSubscriberData(SubscriberMetadataIdentifier subscriberMetadataIdentifier, String urlListStr) {
        storeMetadata(subscriberMetadataIdentifier,urlListStr);
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, URL url){
        String path = buildPath(DEFAULT_MAPPING_GROUP,serviceKey);
        return getAppNames(dubboConfig.getProperty(path,null));
    }

    @Override
    public Set<String> getServiceAppMapping(String serviceKey, MappingListener listener, URL url) {
        String path = buildPath(DEFAULT_MAPPING_GROUP,serviceKey);
        if(null == casListeners.get(path)){
            addCasServiceMappingListener(path,listener);
        }
        return getAppNames(dubboConfig.getProperty(path,null));
    }


    @Override
    public boolean registerServiceAppMapping(String key, String group, String content, Object ticket) {
        try {
            String path = buildPath(group,key);
            OpenItemDTO openItemDTO = new OpenItemDTO();
            openItemDTO.setKey(path);
            openItemDTO.setValue(content);
            openItemDTO.setDataChangeCreatedBy(APOLLO_USER_ID);
            openItemDTO.setComment("registerServiceAppMapping");
            client.createOrUpdateItem(configAppId,configEnv,configCluster,configNamespace,openItemDTO);
            return true;
        }catch (Throwable e){
            logger.warn("Failed to register mapping from apollo.",e);
            return false;
        }

    }

    @Override
    protected String doGetSubscribedURLs(SubscriberMetadataIdentifier subscriberMetadataIdentifier) {
        String key = buildKey(subscriberMetadataIdentifier);
        return dubboConfig.getProperty(key, null);
    }

    public class ApolloListener implements ConfigChangeListener {

        private Set<MappingListener> listeners = new CopyOnWriteArraySet<>();

        ApolloListener(){}

        public void addListener(MappingListener listener){
            listeners.add(listener);
        }

        @Override
        public void onChange(ConfigChangeEvent changeEvent) {
            for (String key : changeEvent.changedKeys()) {
                ConfigChange change = changeEvent.getChange(key);
                if ("".equals(change.getNewValue())) {
                    logger.warn("an empty rule is received for " + key + ", the current working rule is " +
                        change.getOldValue() + ", the empty rule will not take effect.");
                    return;
                }
                Set<String> apps = getAppNames(change.getNewValue());
                MappingChangedEvent event = new MappingChangedEvent(key,apps);
                listeners.forEach(listener -> listener.onEvent(event));
            }
        }
    }
}
