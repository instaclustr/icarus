package com.instaclustr.cassandra.sidecar.service;

import com.google.inject.Inject;
import com.instaclustr.operations.FunctionWithEx;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class CassandraSchemaVersionService {

    private final CassandraJMXService cassandraJMXService;

    @Inject
    public CassandraSchemaVersionService(final CassandraJMXService cassandraJMXService) {
        this.cassandraJMXService = cassandraJMXService;
    }

    public CassandraSchemaVersion getCassandraSchemaVersion() {
        final CassandraSchemaVersion version = new CassandraSchemaVersion();

        try {
            final String schemaVersion = cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, String>() {
                @Override
                public String apply(final StorageServiceMBean ssMBean) throws Exception {
                    return ssMBean.getSchemaVersion();
                }
            });

            version.setSchemaVersion(schemaVersion);
        } catch (final Exception ex) {
            version.setException(ex);
        }

        return version;
    }

    public static class CassandraSchemaVersion {

        private String schemaVersion;

        private Exception exception;

        public String getSchemaVersion() {
            return schemaVersion;
        }

        public void setSchemaVersion(final String schemaVersion) {
            this.schemaVersion = schemaVersion;
        }

        public Exception getException() {
            return exception;
        }

        public void setException(final Exception exception) {
            this.exception = exception;
        }
    }
}
