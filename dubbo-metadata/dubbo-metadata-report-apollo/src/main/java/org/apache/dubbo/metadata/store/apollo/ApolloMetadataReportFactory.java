package org.apache.dubbo.metadata.store.apollo;


import org.apache.dubbo.common.URL;
import org.apache.dubbo.metadata.report.MetadataReport;
import org.apache.dubbo.metadata.report.support.AbstractMetadataReportFactory;

public class ApolloMetadataReportFactory extends AbstractMetadataReportFactory {
    @Override
    protected MetadataReport createMetadataReport(URL url) {
        return new ApolloMetadataReport(url);
    }
}
