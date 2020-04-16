package com.instaclustr.cassandra.sidecar.operations.refresh;

import java.time.Instant;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;

public class RefreshOperation extends Operation<RefreshOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RefreshOperation.class);

    private final CassandraJMXService cassandraJMXService;

    @Inject
    protected RefreshOperation(final CassandraJMXService cassandraJMXService,
                               @Assisted final RefreshOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
    }

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialisation from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private RefreshOperation(@JsonProperty("id") final UUID id,
                             @JsonProperty("creationTime") final Instant creationTime,
                             @JsonProperty("state") final State state,
                             @JsonProperty("failureCause") final Throwable failureCause,
                             @JsonProperty("progress") final float progress,
                             @JsonProperty("startTime") final Instant startTime,
                             @JsonProperty("keyspace") final String keyspace,
                             @JsonProperty("table") final String table) {
        super(id, creationTime, state, failureCause, progress, startTime, new RefreshOperationRequest(keyspace, table));
        cassandraJMXService = null;
    }

    @Override
    protected void run0() throws Exception {
        assert cassandraJMXService != null;

        cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Object>() {
            @Override
            public Object apply(final StorageServiceMBean object) throws Exception {

                object.loadNewSSTables(request.keyspace, request.table);

                logger.info(String.format("Refreshed table %s in keyspace %s", request.table, request.keyspace));

                return null;
            }
        });
    }
}
