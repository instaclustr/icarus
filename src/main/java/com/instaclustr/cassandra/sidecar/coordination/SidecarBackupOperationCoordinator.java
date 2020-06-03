package com.instaclustr.cassandra.sidecar.coordination;

import static com.instaclustr.cassandra.sidecar.coordination.CoordinationUtils.constructSidecars;
import static com.instaclustr.cassandra.sidecar.coordination.CoordinationUtils.getEndpoints;
import static com.instaclustr.cassandra.sidecar.coordination.CoordinationUtils.getEndpointsDCs;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.cassandra.backup.guice.BackuperFactory;
import com.instaclustr.cassandra.backup.guice.BucketServiceFactory;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.cassandra.backup.impl.backup.coordination.BaseBackupOperationCoordinator;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient.OperationResult;
import com.instaclustr.operations.GlobalOperationProgressTracker;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.operations.ResultGatherer;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SidecarBackupOperationCoordinator extends BaseBackupOperationCoordinator {

    private static final int MAX_NUMBER_OF_CONCURRENT_OPERATIONS = Integer.parseInt(System.getProperty("instaclustr.sidecar.operations.executor.size", "100"));

    private static final Logger logger = LoggerFactory.getLogger(SidecarBackupOperationCoordinator.class);

    private final OperationsService operationsService;
    private final SidecarSpec sidecarSpec;
    private final ExecutorServiceSupplier executorServiceSupplier;
    private final ObjectMapper objectMapper;

    @Inject
    public SidecarBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                             final Map<String, BackuperFactory> backuperFactoryMap,
                                             final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                             final OperationsService operationsService,
                                             final SidecarSpec sidecarSpec,
                                             final ExecutorServiceSupplier executorServiceSupplier,
                                             final ObjectMapper objectMapper) {
        super(cassandraJMXService, backuperFactoryMap, bucketServiceFactoryMap);
        this.operationsService = operationsService;
        this.sidecarSpec = sidecarSpec;
        this.executorServiceSupplier = executorServiceSupplier;
        this.objectMapper = objectMapper;
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) throws OperationCoordinatorException {

        /*
         * I receive a request
         *  If it is a global request, I will be coordinator
         *  otherwise just execute that request
         */

        // if it is not global request, there might be at most one global request running
        // and no other restore operations can run, so this means there might be at most one
        // global request running at this node, together with this "normal" restore operation - hence two.
        //
        // this node can be a coordinator of a global request and it can as well receive "normal" restoration request phase
        // so there is a valid case that this node will be running a global request and restoration phase simultaneously
        // hence there will be up to two operations of "restore" type and at most one of them is global

        if (!operation.request.globalRequest) {

            final List<UUID> restoreUUIDs = operationsService.allRunningOfType("backup");

            if (restoreUUIDs.size() > 2) {
                throw new IllegalStateException("There are more than two concurrent backup operations running!");
            }

            int normalRequests = 0;

            for (final UUID uuid : restoreUUIDs) {
                final Optional<Operation> operationOptional = operationsService.operation(uuid);

                if (!operationOptional.isPresent()) {
                    throw new IllegalStateException(format("received empty optional for uuid %s", uuid.toString()));
                }

                final Operation op = operationOptional.get();

                if (!(op.request instanceof BackupOperationRequest)) {
                    throw new IllegalStateException(format("Received request is not of type %s", BackupOperationRequest.class));
                }

                BackupOperationRequest request = (BackupOperationRequest) op.request;

                if (!request.globalRequest) {
                    normalRequests += 1;
                }
            }

            if (normalRequests == 2) {
                throw new IllegalStateException("We can not run two normal backup requests simultaneously.");
            }

            return super.coordinate(operation);
        }

        // if it is a global request, we will coordinate whole backup across a cluster in this operation
        // when this operation finishes, whole cluster will be restored.

        // first we have to make some basic checks, e.g. we can be the only global backup operation on this node
        // and no other restore operations (even partial) can run simultaneously

        final List<UUID> restoreUUIDs = operationsService.allRunningOfType("backup");

        if (restoreUUIDs.size() != 1) {
            throw new IllegalStateException("There is more than one running backup operation.");
        }

        if (!restoreUUIDs.get(0).equals(operation.id)) {
            throw new IllegalStateException("ID of a running operation does not equal to ID of this backup operation!");
        }

        final Map<InetAddress, UUID> endpoints = getEndpoints(cassandraJMXService, operation.request.dc);
        final Map<InetAddress, String> endpointDCs = getEndpointsDCs(cassandraJMXService, endpoints.keySet());
        final Map<InetAddress, SidecarClient> sidecarClientMap = constructSidecars(endpoints, endpointDCs, sidecarSpec, objectMapper);

        logger.info("Executing backup requests against " + sidecarClientMap.toString());

        final BackupRequestPreparation backupRequestPreparation = (client, globalRequest) -> {

            try {
                if (!client.getHostId().isPresent()) {
                    throw new OperationCoordinatorException(format("There is not any hostId for client %s", client.getHost()));
                }

                final BackupOperationRequest clonedRequest = (BackupOperationRequest) globalRequest.clone();
                final BackupOperation backupOperation = new BackupOperation(clonedRequest);
                backupOperation.request.globalRequest = false;

                backupOperation.request.storageLocation = StorageLocation.updateNodeId(backupOperation.request.storageLocation, client.getHostId().get());
                backupOperation.request.storageLocation = StorageLocation.updateDatacenter(backupOperation.request.storageLocation, client.getDc());

                return backupOperation;
            } catch (final Exception ex) {
                throw new OperationCoordinatorException(format("Unable to prepare backup operation for client %s.", client.getHost()), ex);
            }
        };

        return executeDistributedBackup(operation, sidecarClientMap, backupRequestPreparation);
    }

    private interface BackupRequestPreparation {

        Operation<BackupOperationRequest> prepare(final SidecarClient client, final BackupOperationRequest globalRequest) throws OperationCoordinatorException;
    }

    private BackupPhaseResultGatherer executeDistributedBackup(final Operation<BackupOperationRequest> globalOperation,
                                                               final Map<InetAddress, SidecarClient> sidecarClientMap,
                                                               final BackupRequestPreparation requestPreparation) throws OperationCoordinatorException {
        final ExecutorService executorService = executorServiceSupplier.get(MAX_NUMBER_OF_CONCURRENT_OPERATIONS);

        final BackupPhaseResultGatherer resultGatherer = new BackupPhaseResultGatherer();

        try {
            final List<BackupOperationCallable> callables = new ArrayList<>();
            final GlobalOperationProgressTracker progressTracker = new GlobalOperationProgressTracker(globalOperation, sidecarClientMap.entrySet().size());

            // create

            for (final Map.Entry<InetAddress, SidecarClient> entry : sidecarClientMap.entrySet()) {
                callables.add(new BackupOperationCallable(requestPreparation.prepare(entry.getValue(), globalOperation.request),
                                                          entry.getValue(),
                                                          progressTracker));
            }

            // submit & gather results

            allOf(callables.stream().map(c -> supplyAsync(c, executorService).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    logger.warn(format("Backup against %s has failed.", result.request.storageLocation));
                    resultGatherer.gather(result, throwable);
                }
            })).toArray(CompletableFuture<?>[]::new)).get();
        } catch (ExecutionException | InterruptedException ex) {
            ex.printStackTrace();
            resultGatherer.gather(globalOperation, new OperationCoordinatorException("Unable to coordinate backup!", ex));
        } finally {
            executorService.shutdownNow();
        }

        return resultGatherer;
    }

    private static class BackupOperationCallable extends OperationCallable<BackupOperation, BackupOperationRequest> {

        public BackupOperationCallable(final Operation<BackupOperationRequest> operation,
                                       final SidecarClient sidecarClient,
                                       final GlobalOperationProgressTracker progressTracker) {
            super(operation, sidecarClient, progressTracker, "backup");
        }

        public OperationResult<BackupOperation> sendOperation() {
            return super.sidecarClient.backup(super.operation.request);
        }
    }
}
