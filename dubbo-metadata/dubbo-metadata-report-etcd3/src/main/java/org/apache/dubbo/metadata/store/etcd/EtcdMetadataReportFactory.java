package org.apache.dubbo.metadata.store.etcd;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReportFactory;

public class EtcdMetadataReportFactory extends AbstractMetadataReportFactory {
    @Override
    protected MetadataReport createMetadataReport(URL url) {
        return new EtcdMetadataReport(url);
    }
}
