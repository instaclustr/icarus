package com.instaclustr.icarus.rest;

import static com.instaclustr.operations.Operation.State.COMPLETED;
import static com.instaclustr.operations.Operation.State.FAILED;
import static com.instaclustr.operations.Operation.State.PENDING;
import static com.instaclustr.operations.Operation.State.RUNNING;
import static java.lang.String.format;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.CREATED;
import static org.awaitility.Awaitility.await;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.impl._import.ImportOperation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperation;
import com.instaclustr.esop.impl.backup.BackupCommitLogsOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupOperation;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperation;
import com.instaclustr.esop.impl.restore.RestoreCommitLogsOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperation;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.truncate.TruncateOperation;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.icarus.operations.cleanup.CleanupOperation;
import com.instaclustr.icarus.operations.cleanup.CleanupOperationRequest;
import com.instaclustr.icarus.operations.decommission.DecommissionOperation;
import com.instaclustr.icarus.operations.decommission.DecommissionOperationRequest;
import com.instaclustr.icarus.operations.drain.DrainOperation;
import com.instaclustr.icarus.operations.drain.DrainOperationRequest;
import com.instaclustr.icarus.operations.flush.FlushOperation;
import com.instaclustr.icarus.operations.flush.FlushOperationRequest;
import com.instaclustr.icarus.operations.rebuild.RebuildOperation;
import com.instaclustr.icarus.operations.rebuild.RebuildOperationRequest;
import com.instaclustr.icarus.operations.refresh.RefreshOperation;
import com.instaclustr.icarus.operations.refresh.RefreshOperationRequest;
import com.instaclustr.icarus.operations.restart.RestartOperation;
import com.instaclustr.icarus.operations.restart.RestartOperationRequest;
import com.instaclustr.icarus.operations.icarus.StopIcarusOperation;
import com.instaclustr.icarus.operations.icarus.StopIcarusOperationRequest;
import com.instaclustr.icarus.operations.scrub.ScrubOperation;
import com.instaclustr.icarus.operations.scrub.ScrubOperationRequest;
import com.instaclustr.icarus.operations.upgradesstables.UpgradeSSTablesOperation;
import com.instaclustr.icarus.operations.upgradesstables.UpgradeSSTablesOperationRequest;
import com.instaclustr.icarus.service.CassandraService.CassandraSchemaVersion;
import com.instaclustr.icarus.service.CassandraStatusService.Status;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.State;
import com.instaclustr.operations.OperationRequest;
import org.awaitility.Duration;
import org.awaitility.core.ConditionFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcarusClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(IcarusClient.class);

    private final String rootUrl;
    private final Client client;

    private final WebTarget statusWebTarget;
    private final WebTarget operationsWebTarget;
    private final WebTarget cassandraSchemaWebTarget;
    private final WebTarget cassandraVersionWebTarget;
    private final WebTarget icarusVersionWebTarget;
    private final WebTarget cassandraTopologyWebTarget;

    private final int port;
    private final String hostAddress;
    private final String clusterName;
    private final String dc; // datacenter as Cassandra sees it
    private final UUID hostId; // hostId as Cassandra sees it
    private final ObjectMapper objectMapper;

    private IcarusClient(final Builder builder, final Client client) {
        this.hostAddress = builder.hostAddress;
        this.port = builder.port;

        if (client == null) {
            this.client = ClientBuilder.newBuilder().build();
        } else {
            this.client = client;
        }

        rootUrl = format("http://%s:%s", builder.hostAddress, builder.port);

        statusWebTarget = this.client.target(format("%s/status", rootUrl));
        operationsWebTarget = this.client.target(format("%s/operations", rootUrl));

        cassandraSchemaWebTarget = this.client.target(format("%s/version/schema", rootUrl));
        cassandraVersionWebTarget = this.client.target(format("%s/version/cassandra", rootUrl));
        icarusVersionWebTarget = this.client.target(format("%s/version/sidecar", rootUrl));
        cassandraTopologyWebTarget = this.client.target(format("%s/topology", rootUrl));

        this.clusterName = builder.clusterName;
        this.dc = builder.dc;
        this.hostId = builder.hostId;

        this.objectMapper = builder.objectMapper;
    }

    private IcarusClient(final Builder builder, final ResourceConfig resourceConfig) {
        this(builder, ClientBuilder.newBuilder().withConfig(resourceConfig).build());
    }

    @Override
    public String toString() {
        return "SidecarClient{" +
                "port=" + port +
                ", hostAddress='" + hostAddress + '\'' +
                ", cluster=" + clusterName +
                ", dc='" + dc + '\'' +
                ", hostId=" + hostId +
                '}';
    }

    public StatusResult getStatus() {
        try {
            final Response response = statusWebTarget.request(APPLICATION_JSON).get();
            final Status status = response.readEntity(Status.class);
            return new StatusResult(status, response);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CassandraSchemaVersion getCassandraSchemaVersion() {
        try {
            final Response response = cassandraSchemaWebTarget.request(APPLICATION_JSON).get();
            return response.readEntity(CassandraSchemaVersion.class);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public CassandraVersion getCassandraVersion() {
        try {
            final Response response = cassandraVersionWebTarget.request(APPLICATION_JSON).get();
            return response.readEntity(CassandraVersion.class);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public String getIcarusVersion() {
        try {
            return icarusVersionWebTarget.request(APPLICATION_JSON).get().readEntity(String.class);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public ClusterTopology getCassandraClusterTopology(final String dc) {
        try {
            Response response;

            if (dc != null) {
                response = cassandraTopologyWebTarget.path(dc).request(APPLICATION_JSON).get();
            } else {
                response = cassandraTopologyWebTarget.request(APPLICATION_JSON).get();
            }

            return response.readEntity(ClusterTopology.class);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public Optional<UUID> getHostId() {
        return Optional.ofNullable(hostId);
    }

    public String getDc() {
        return dc;
    }

    public int getPort() {
        return port;
    }

    public String getHost() {
        return hostAddress;
    }

    public String getClusterName() {
        return clusterName;
    }

    public OperationResult<CleanupOperation> cleanup(final CleanupOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<DecommissionOperation> decommission(final DecommissionOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<DecommissionOperation> decommission(Boolean force) {
        return decommission(new DecommissionOperationRequest(force));
    }

    public OperationResult<DecommissionOperation> decommission() {
        return decommission(false);
    }

    public OperationResult<RebuildOperation> rebuild(final RebuildOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<ScrubOperation> scrub(final ScrubOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<UpgradeSSTablesOperation> upgradeSSTables(final UpgradeSSTablesOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<DrainOperation> drain(final DrainOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<DrainOperation> drain() {
        return drain(new DrainOperationRequest());
    }

    public OperationResult<RestartOperation> restart(final RestartOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<StopIcarusOperation> stopSidecarOperation(final StopIcarusOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<RefreshOperation> refresh(final RefreshOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<FlushOperation> flush(final FlushOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<TruncateOperation> truncate(final TruncateOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<ImportOperation> importOperation(final ImportOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<BackupOperation> backup(final BackupOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<BackupCommitLogsOperation> backupCommitLogs(final BackupCommitLogsOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<RestoreOperation> restore(final RestoreOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<RestoreCommitLogsOperation> restoreCommitLogs(final RestoreCommitLogsOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<RemoveBackupOperation> remove(RemoveBackupRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public OperationResult<ListOperation> list(ListOperationRequest operationRequest) {
        return performOperationSubmission(operationRequest);
    }

    public Collection<Operation<?>> getOperations() {
        return getOperations(ImmutableSet.of(), ImmutableSet.of());
    }

    public Collection<Operation<?>> getOperations(final Set<String> operations, final Set<State> states) {

        WebTarget webTarget = client.target(format("%s/operations", rootUrl));

        if (operations != null && !operations.isEmpty()) {
            webTarget = webTarget.queryParam("type", operations.toArray());
        }

        if (states != null && !states.isEmpty()) {
            webTarget = webTarget.queryParam("state", states.stream().map(State::name).toArray());
        }

        return Arrays.asList(webTarget.request(APPLICATION_JSON).get(Operation[].class));
    }

    public static String responseEntityToString(final Response response) throws IOException {
        return CharStreams.toString(new InputStreamReader((InputStream) response.getEntity()));
    }

    public synchronized <T extends OperationRequest, O extends Operation<?>> OperationResult<O> performOperationSubmission(final T operationRequest) {

        final Response post = operationsWebTarget.request(APPLICATION_JSON).post(Entity.json(operationRequest));

        String stringBody;

        try {
            stringBody = post.readEntity(String.class);
            logger.info("\n" + stringBody);
        } catch (final Exception ex) {
            throw new IllegalStateException("Unable to read operation back!", ex);
        }

        if (post.getStatusInfo().toEnum() != CREATED) {
            return new OperationResult<O>(null, post);
        }

        final JavaType javaType = objectMapper.getTypeFactory().constructParametricType(Operation.class, operationRequest.getClass());

        return new OperationResult<>((O) parseString(stringBody, javaType), post);
    }

    public Operation<?> getOperation(final UUID operationId) {
        return operationsWebTarget.path(operationId.toString()).request(APPLICATION_JSON).get(Operation.class);
    }

    public <T extends OperationRequest> Operation<T> getOperation(final UUID operationId, final Class<T> operationRequest) {
        final String stringBody = operationsWebTarget.path(operationId.toString()).request(APPLICATION_JSON).get().readEntity(String.class);
        final JavaType javaType = objectMapper.getTypeFactory().constructParametricType(Operation.class, operationRequest);

        return (Operation<T>) parseString(stringBody, javaType);
    }

    public <T extends OperationRequest> Operation<T> getOperation(final UUID operationId, final T operationRequest) {
        return getOperation(operationId, (Class<T>) operationRequest.getClass());
    }

    private Object parseString(final String body, final JavaType javaType) {
        Object o = null;

        try {
            o = objectMapper.readValue(body, javaType);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return o;
    }

    @Override
    public void close() {
        logger.debug("Closing Sidecar client {}", this.getHost());
        client.close();
    }

    public static final class Builder {

        private String hostAddress = "localhost";

        private int port = 8080;

        private String clusterName;
        public String dc;
        private UUID hostId;

        private ObjectMapper objectMapper;

        public Builder withInetAddress(final InetAddress inetAddress) {
            return withHostAddress(inetAddress.getHostAddress());
        }

        public Builder withHostAddress(final String hostAddress) {
            this.hostAddress = hostAddress;
            return this;
        }

        public Builder withHostId(final UUID hostId) {
            this.hostId = hostId;
            return this;
        }

        public Builder withDc(final String dc) {
            this.dc = dc;
            return this;
        }

        public Builder withClusterName(final String clusterName) {
            this.clusterName = clusterName;
            return this;
        }

        public Builder withPort(final int port) {
            this.port = port;
            return this;
        }

        public Builder withObjectMapper(final ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
            return this;
        }

        public IcarusClient build(final Client client) {
            return new IcarusClient(this, client);
        }

        public IcarusClient build(final ResourceConfig resourceConfig) {
            return new IcarusClient(this, resourceConfig);
        }

        public IcarusClient build() {
            return build(ClientBuilder.newBuilder().build());
        }
    }

    public static class StatusResult {

        public final Status status;
        public final Response response;

        public StatusResult(final Status status, final Response response) {
            this.status = status;
            this.response = response;
        }
    }

    public static class OperationResult<O extends Operation<?>> {

        public final O operation;
        public final Response response;

        public OperationResult(final O operation, final Response response) {
            this.operation = operation;
            this.response = response;
        }
    }

    public void waitForCompleted(final OperationResult<? extends Operation<?>> operationResult, final Duration duration) {
        waitForState(operationResult, COMPLETED, duration);
    }

    public void waitForCompleted(final OperationResult<? extends Operation<?>> operationResult) {
        waitForState(operationResult, COMPLETED);
    }

    public void waitForFailed(final OperationResult<? extends Operation<?>> operationResult, final Duration duration) {
        waitForState(operationResult, FAILED, duration);
    }

    public void waitForFailed(final OperationResult<? extends Operation<?>> operationResult) {
        waitForState(operationResult, FAILED);
    }

    public void waitForPending(final OperationResult<? extends Operation<?>> operationResult, final Duration duration) {
        waitForState(operationResult, PENDING, duration);
    }

    public void waitForPending(final OperationResult<? extends Operation<?>> operationResult) {
        waitForState(operationResult, PENDING);
    }

    public void waitForRunning(final OperationResult<? extends Operation<?>> operationResult, final Duration duration) {
        waitForState(operationResult, RUNNING, duration);
    }

    public void waitForRunning(final OperationResult<? extends Operation<?>> operationResult) {
        waitForState(operationResult, RUNNING);
    }

    public void waitForFinished(final OperationResult<? extends Operation<?>> operationResult, final Duration duration) {

        final ConditionFactory conditionFactory = duration == null ? await() : await().atMost(duration);

        conditionFactory.until(() -> {
            final Operation<?> operation = getOperation(operationResult.operation.id);

            final State returnedState = operation.state;

            return State.TERMINAL_STATES.contains(returnedState);
        });
    }

    public void waitForFinished(final OperationResult<? extends Operation<?>> operationResult) {
        waitForFinished(operationResult, null);
    }

    public void waitForState(final OperationResult<? extends Operation<?>> operationResult, final State state, final Duration duration) {

        final ConditionFactory conditionFactory = duration == null ? await() : await().atMost(duration);

        conditionFactory.timeout(1, TimeUnit.HOURS).pollInterval(5, TimeUnit.SECONDS).until(() -> {

            if (operationResult == null) {
                return false;
            }

            if (operationResult.operation == null) {
                return false;
            }

            if (operationResult.operation.id == null) {
                return false;
            }

            final Operation<?> operation = getOperation(operationResult.operation.id);

            if (operation == null) {
                return false;
            }

            final State returnedState = operation.state;

            if (state == FAILED && state == returnedState) {
                return true;
            }

            if (state == PENDING && state == returnedState) {
                return true;
            }

            if (state == RUNNING && state == returnedState) {
                return true;
            }

            if (returnedState == FAILED) {
                return false;
            } else if (returnedState == PENDING || returnedState == RUNNING) {
                return false;
            } else {
                return returnedState == state;
            }
        });
    }

    public void waitForState(OperationResult<? extends Operation<?>> operationResult, State state) {
        waitForState(operationResult, state, null);
    }
}
