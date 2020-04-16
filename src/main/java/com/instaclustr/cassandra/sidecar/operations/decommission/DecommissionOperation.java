package com.instaclustr.cassandra.sidecar.operations.decommission;

import java.time.Instant;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra4.Cassandra4StorageServiceMBean;

public class DecommissionOperation extends Operation<DecommissionOperationRequest> {
    private static final Logger logger = LoggerFactory.getLogger(DecommissionOperation.class);
    private final CassandraJMXService cassandraJMXService;
    private final Provider<CassandraVersion> cassandraVersionProvider;

    @Inject
    public DecommissionOperation(final CassandraJMXService cassandraJMXService,
                                 final Provider<CassandraVersion> cassandraVersionProvider,
                                 @Assisted final DecommissionOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersionProvider = cassandraVersionProvider;
    }

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialisation from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private DecommissionOperation(@JsonProperty("id") final UUID id,
                                  @JsonProperty("creationTime") final Instant creationTime,
                                  @JsonProperty("state") final State state,
                                  @JsonProperty("failureCause") final Throwable failureCause,
                                  @JsonProperty("progress") final float progress,
                                  @JsonProperty("startTime") final Instant startTime,
                                  @JsonProperty("force") final Boolean force) {
        super(id, creationTime, state, failureCause, progress, startTime, new DecommissionOperationRequest(force));
        cassandraJMXService = null;
        cassandraVersionProvider = null;
    }

    @Override
    protected void run0() throws Exception {
        assert cassandraJMXService != null;
        assert cassandraVersionProvider != null;

        if (cassandraVersionProvider.get().getMajor() == 4) {
            cassandraJMXService.doWithCassandra4StorageServiceMBean(new FunctionWithEx<Cassandra4StorageServiceMBean, Void>() {
                @Override
                public Void apply(final Cassandra4StorageServiceMBean object) throws Exception {
                    object.decommission(request.force == null ? true : request.force);
                    return null;
                }
            });
        } else {
            cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Void>() {
                @Override
                public Void apply(final StorageServiceMBean object) throws Exception {
                    object.decommission();
                    return null;
                }
            });
        }
    }
}
