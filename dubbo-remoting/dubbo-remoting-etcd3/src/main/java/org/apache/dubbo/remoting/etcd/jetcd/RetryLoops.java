package org.apache.dubbo.remoting.etcd.jetcd;

import io.grpc.Status;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.etcd.RetryPolicy;
import org.apache.dubbo.remoting.etcd.option.OptionUtil;

import java.util.concurrent.Callable;

public class RetryLoops {

    private final long startTimeMs = System.currentTimeMillis();
    private boolean isDone = false;
    private int retriedCount = 0;
    private Logger logger = LoggerFactory.getLogger(RetryLoops.class);

    public static <R> R invokeWithRetry(Callable<R> task, RetryPolicy retryPolicy) throws Exception {
        R result = null;
        RetryLoops retryLoop = new RetryLoops();
        while (retryLoop.shouldContinue()) {
            try {
                result = task.call();
                retryLoop.complete();
            } catch (Exception e) {
                retryLoop.fireException(e, retryPolicy);
            }
        }
        return result;
    }

    public void fireException(Exception e, RetryPolicy retryPolicy) throws Exception {

        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }

        boolean rethrow = true;
        if (isRetryException(e)
                && retryPolicy.shouldRetry(retriedCount++, System.currentTimeMillis() - startTimeMs, true)) {
            rethrow = false;
        }

        if (rethrow) {
            throw e;
        }
    }

    private boolean isRetryException(Throwable e) {
        Status status = Status.fromThrowable(e);
        if (OptionUtil.isRecoverable(status)) {
            return true;
        }

        return false;
    }

    public boolean shouldContinue() {
        return !isDone;
    }

    public void complete() {
        isDone = true;
    }

}
