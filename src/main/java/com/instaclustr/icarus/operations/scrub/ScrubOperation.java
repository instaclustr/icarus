package com.instaclustr.icarus.operations.scrub;

import javax.inject.Provider;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.operations.FunctionWithEx;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationFailureException;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import jmx.org.apache.cassandra.service.cassandra2.Cassandra2StorageServiceMBean;
import jmx.org.apache.cassandra.service.cassandra3.StorageServiceMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScrubOperation extends Operation<ScrubOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(ScrubOperation.class);

    private final CassandraJMXService cassandraJMXService;
    private final Provider<CassandraVersion> cassandraVersionProvider;

    @Inject
    public ScrubOperation(final CassandraJMXService cassandraJMXService,
                          final Provider<CassandraVersion> cassandraVersionProvider,
                          @Assisted final ScrubOperationRequest request) {
        super(request);

        this.cassandraJMXService = cassandraJMXService;
        this.cassandraVersionProvider = cassandraVersionProvider;
    }

    // this constructor is not meant to be instantiated manually
    // and it fulfills the purpose of deserialisation from JSON string to an Operation object, currently just for testing purposes
    @JsonCreator
    private ScrubOperation(@JsonProperty("type") final String type,
                           @JsonProperty("id") final UUID id,
                           @JsonProperty("creationTime") final Instant creationTime,
                           @JsonProperty("state") final State state,
                           @JsonProperty("failureCause") final Throwable failureCause,
                           @JsonProperty("progress") final float progress,
                           @JsonProperty("startTime") final Instant startTime,
                           @JsonProperty("disableSnapshot") final boolean disableSnapshot,
                           @JsonProperty("skipCorrupted") final boolean skipCorrupted,
                           @JsonProperty("noValidate") final boolean noValidate,
                           @JsonProperty("reinsertOverflowedTTL") final boolean reinsertOverflowedTTL,
                           @JsonProperty("jobs") final int jobs,
                           @JsonProperty("keyspace") final String keyspace,
                           @JsonProperty("tables") final Set<String> tables) {
        super(type, id, creationTime, state, failureCause, progress, startTime, new ScrubOperationRequest(type,
                                                                                                          disableSnapshot,
                                                                                                          skipCorrupted,
                                                                                                          noValidate,
                                                                                                          reinsertOverflowedTTL,
                                                                                                          jobs,
                                                                                                          keyspace,
                                                                                                          tables));
        cassandraJMXService = null;
        cassandraVersionProvider = null;
    }

    // scrubbing for Cassandra version 2
    private void scrubCassandra2() throws Exception {
        cassandraJMXService.doWithCassandra2StorageServiceMBean(new FunctionWithEx<Cassandra2StorageServiceMBean, Object>() {
            @Override
            public Object apply(final Cassandra2StorageServiceMBean ssMBean) throws Exception {
                return ssMBean.scrub(request.disableSnapshot,
                                     request.skipCorrupted,
                                     !request.noValidate,
                                     request.reinsertOverflowedTTL,
                                     request.jobs,
                                     request.keyspace,
                                     request.tables == null ? new String[]{} : request.tables.toArray(new String[0]));
            }
        });
    }

    // scrubbing for Cassandra 3 and 4
    private void scrubCassandra() throws Exception {
        final int concurrentCompactors = cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Integer>() {
            @Override
            public Integer apply(final StorageServiceMBean ssMBean) {
                return ssMBean.getConcurrentCompactors();
            }
        });

        if (request.jobs > concurrentCompactors) {
            logger.info(String.format("jobs (%d) is bigger than configured concurrent_compactors (%d) on this host, using at most %d threads",
                                      request.jobs,
                                      concurrentCompactors,
                                      concurrentCompactors));
        }

        final int result = cassandraJMXService.doWithStorageServiceMBean(new FunctionWithEx<StorageServiceMBean, Integer>() {
            @Override
            public Integer apply(final StorageServiceMBean object) throws Exception {
                return object.scrub(request.disableSnapshot,
                                    request.skipCorrupted,
                                    !request.noValidate,
                                    request.reinsertOverflowedTTL,
                                    request.jobs,
                                    request.keyspace,
                                    request.tables == null ? new String[]{} : request.tables.toArray(new String[0]));
            }
        });

        switch (result) {
            case 1:
                throw new OperationFailureException("Aborted scrubbing at least one table in keyspace " + request.keyspace
                                                            + ", check server logs for more information.");
            case 2:
                throw new OperationFailureException("Failed marking some sstables compacting in keyspace " + request.keyspace
                                                            + ", check server logs for more information");
        }
    }

    @Override
    protected void run0() throws Exception {

        assert cassandraJMXService != null;

        if (cassandraVersionProvider.get().getMajor() == 2) {
            scrubCassandra2();
        } else {
            scrubCassandra();
        }
    }
}
