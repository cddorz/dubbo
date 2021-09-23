package org.apache.dubbo.remoting.etcd;


import static org.apache.dubbo.remoting.Constants.DEFAULT_IO_THREADS;

public interface Constants {
    String ETCD3_NOTIFY_MAXTHREADS_KEYS = "etcd3.notify.maxthreads";

    int DEFAULT_ETCD3_NOTIFY_THREADS = DEFAULT_IO_THREADS;

    String DEFAULT_ETCD3_NOTIFY_QUEUES_KEY = "etcd3.notify.queues";

    int DEFAULT_GRPC_QUEUES = 300_0000;

    String RETRY_PERIOD_KEY = "retry.period";

    int DEFAULT_RETRY_PERIOD = 5 * 1000;

    int DEFAULT_SESSION_TIMEOUT = 60 * 1000;

    String HTTP_SUBFIX_KEY = "://";

    String HTTP_KEY = "http://";

    int DEFAULT_KEEPALIVE_TIMEOUT = DEFAULT_SESSION_TIMEOUT / 2;

    String SESSION_TIMEOUT_KEY = "session";

    int DEFAULT_RECONNECT_PERIOD = 3 * 1000;

    String ROUTERS_CATEGORY = "routers";

    String PROVIDERS_CATEGORY = "providers";

    String CONSUMERS_CATEGORY = "consumers";

    String CONFIGURATORS_CATEGORY = "configurators";
}
