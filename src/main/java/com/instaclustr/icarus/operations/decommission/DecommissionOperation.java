package com.instaclustr.icarus.operations.decommission;

import javax.inject.Inject;
import javax.inject.Provider;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

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

    // this constructor is not meant to be instantiated manually,
    // and it fulfills the purpose of deserialization from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private DecommissionOperation(@JsonProperty("type") final String type,
                                  @JsonProperty("id") final UUID id,
                                  @JsonProperty("creationTime") final Instant creationTime,
                                  @JsonProperty("state") final State state,
                                  @JsonProperty("errors") final List<Error> errors,
                                  @JsonProperty("progress") final float progress,
                                  @JsonProperty("startTime") final Instant startTime,
                                  @JsonProperty("force") final Boolean force) {
        super(type, id, creationTime, state, errors, progress, startTime, new DecommissionOperationRequest(type, force));
        cassandraJMXService = null;
        cassandraVersionProvider = null;
    }

    @Override
    protected void run0() throws Exception {
        assert cassandraJMXService != null;
        assert cassandraVersionProvider != null;

        if (cassandraVersionProvider.get().getMajor() >= 4) {
            cassandraJMXService.doWithCassandra4StorageServiceMBean(new FunctionWithEx<Cassandra4StorageServiceMBean, Void>() {
                @Override
                public Void apply(final Cassandra4StorageServiceMBean object) throws Exception {
                    object.decommission(request.force == null || request.force);
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
