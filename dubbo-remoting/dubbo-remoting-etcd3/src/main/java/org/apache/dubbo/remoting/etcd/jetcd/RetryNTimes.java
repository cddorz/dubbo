package org.apache.dubbo.remoting.etcd.jetcd;

import org.apache.dubbo.remoting.etcd.AbstractRetryPolicy;

import java.util.concurrent.TimeUnit;

public class RetryNTimes extends AbstractRetryPolicy {

    private final long sleepMilliseconds;

    public RetryNTimes(int maxRetried, int sleepTime, TimeUnit unit) {
        super(maxRetried);
        this.sleepMilliseconds = unit.convert(sleepTime, TimeUnit.MILLISECONDS);
    }

    @Override
    protected long getSleepTime(int retried, long elapsed) {
        return sleepMilliseconds;
    }
}
