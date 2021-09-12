package org.apache.dubbo.metadata.store.consul;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReportFactory;

/**
 * @author hly
 * @Description: TODO
 * @create 2021-09-09 20:59
 */
public class ConsulMetadataReportFactory extends AbstractMetadataReportFactory {

    @Override
    protected MetadataReport createMetadataReport(URL url) {
        return new ConsulMetadataReport(url);
    }
}

