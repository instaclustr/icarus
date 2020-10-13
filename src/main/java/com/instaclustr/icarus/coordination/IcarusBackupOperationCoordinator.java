package com.instaclustr.icarus.coordination;

import static com.instaclustr.icarus.coordination.CoordinationUtils.constructSidecars;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import javax.inject.Provider;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.esop.guice.BackuperFactory;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.impl.KeyspaceTable;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.backup.BackupPhaseResultGatherer;
import com.instaclustr.esop.impl.backup.Backuper;
import com.instaclustr.esop.impl.backup.UploadTracker;
import com.instaclustr.esop.impl.backup.coordination.BaseBackupOperationCoordinator;
import com.instaclustr.esop.topology.CassandraClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.rest.IcarusClient.OperationResult;
import com.instaclustr.operations.GlobalOperationProgressTracker;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.ResultGatherer;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcarusBackupOperationCoordinator extends BaseBackupOperationCoordinator {

    private static final int MAX_NUMBER_OF_CONCURRENT_OPERATIONS = Integer.parseInt(System.getProperty("instaclustr.sidecar.operations.executor.size", "100"));

    private static final Logger logger = LoggerFactory.getLogger(IcarusBackupOperationCoordinator.class);

    private final SidecarSpec icarusSpec;
    private final ExecutorServiceSupplier executorServiceSupplier;

    @Inject
    public IcarusBackupOperationCoordinator(final CassandraJMXService cassandraJMXService,
                                            final Provider<CassandraVersion> cassandraVersionProvider,
                                            final Map<String, BackuperFactory> backuperFactoryMap,
                                            final Map<String, BucketServiceFactory> bucketServiceFactoryMap,
                                            final SidecarSpec icarusSpec,
                                            final ExecutorServiceSupplier executorServiceSupplier,
                                            final ObjectMapper objectMapper,
                                            final UploadTracker uploadTracker) {
        super(cassandraJMXService, cassandraVersionProvider, backuperFactoryMap, bucketServiceFactoryMap, objectMapper, uploadTracker);
        this.icarusSpec = icarusSpec;
        this.executorServiceSupplier = executorServiceSupplier;
    }

    @Override
    public ResultGatherer<BackupOperationRequest> coordinate(final Operation<BackupOperationRequest> operation) {

        if (!operation.request.globalRequest) {
            return super.coordinate(operation);
        }

        try {
            KeyspaceTable.checkEntitiesToProcess(operation.request.cassandraDirectory.resolve("data"), operation.request.entities);
        } catch (final Exception ex) {
            logger.error(ex.getMessage());
            return new BackupPhaseResultGatherer().gather(operation, ex);
        }

        ClusterTopology topology;

        try {
            topology = new CassandraClusterTopology(cassandraJMXService, operation.request.dc).act();
        } catch (final Exception ex) {
            return new BackupPhaseResultGatherer().gather(operation, new OperationCoordinatorException("Unable to get cluster topology.", ex));
        }

        logger.info("Resolved datacenter: {}", operation.request.dc == null ? "all of them" : operation.request.dc);

        final Map<InetAddress, IcarusClient> icarusClientMap = constructSidecars(topology.clusterName,
                                                                                 topology.endpoints,
                                                                                 topology.endpointDcs,
                                                                                 icarusSpec,
                                                                                 objectMapper);

        logger.info("Executing backup requests against {}", icarusClientMap.toString());

        operation.request.schemaVersion = topology.schemaVersion;
        operation.request.snapshotTag = resolveSnapshotTag(operation.request, topology.timestamp);

        logger.info("Resolved schema version: {}, ", operation.request.schemaVersion);
        logger.info("Resolved snapshot name : {}, ", operation.request.snapshotTag);

        final BackupRequestPreparation backupRequestPreparation = (client, globalRequest, clusterTopology) -> {

            try {
                if (!client.getHostId().isPresent()) {
                    throw new OperationCoordinatorException(format("There is not any hostId for client %s", client.getHost()));
                }

                final BackupOperationRequest clonedRequest = (BackupOperationRequest) globalRequest.clone();
                final BackupOperation backupOperation = new BackupOperation(clonedRequest);
                backupOperation.request.globalRequest = false;
                // if this is a global request, we upload cluster topology just once, from coordinator
                backupOperation.request.uploadClusterTopology = false;

                backupOperation.request.storageLocation = StorageLocation.update(backupOperation.request.storageLocation,
                                                                                 client.getClusterName(),
                                                                                 client.getDc(),
                                                                                 client.getHostId().get().toString());

                backupOperation.request.storageLocation.globalRequest = false;

                return backupOperation;
            } catch (final Exception ex) {
                throw new OperationCoordinatorException(format("Unable to prepare backup operation for client %s.", client.getHost()), ex);
            }
        };


        final BackupPhaseResultGatherer backupPhaseResultGatherer = executeDistributedBackup(operation,
                                                                                             icarusClientMap,
                                                                                             backupRequestPreparation,
                                                                                             topology);

        if (backupPhaseResultGatherer.hasErrors()) {
            logger.error("Backup operation failed and it is finishing prematurely ...");
            return backupPhaseResultGatherer;
        }

        // we do not look if uploadClusterTopology is true or not, global request coordinator will upload it every time
        try (final Backuper backuper = backuperFactoryMap.get(operation.request.storageLocation.storageProvider).createBackuper(operation.request)) {
            ClusterTopology.upload(backuper, topology, objectMapper, operation.request.snapshotTag);
        } catch (final Exception ex) {
            ex.printStackTrace();
            backupPhaseResultGatherer.gather(operation, new OperationCoordinatorException("Unable to upload topology file", ex));
        }

        return backupPhaseResultGatherer;
    }

    private interface BackupRequestPreparation {

        Operation<BackupOperationRequest> prepare(final IcarusClient client, final BackupOperationRequest globalRequest, final ClusterTopology topology) throws OperationCoordinatorException;
    }

    private BackupPhaseResultGatherer executeDistributedBackup(final Operation<BackupOperationRequest> globalOperation,
                                                               final Map<InetAddress, IcarusClient> icarusClientMap,
                                                               final BackupRequestPreparation requestPreparation,
                                                               final ClusterTopology topology) {
        final ExecutorService executorService = executorServiceSupplier.get(MAX_NUMBER_OF_CONCURRENT_OPERATIONS);

        final BackupPhaseResultGatherer resultGatherer = new BackupPhaseResultGatherer();

        try {
            final List<BackupOperationCallable> callables = new ArrayList<>();
            final GlobalOperationProgressTracker progressTracker = new GlobalOperationProgressTracker(globalOperation, icarusClientMap.entrySet().size());

            // create

            for (final Map.Entry<InetAddress, IcarusClient> entry : icarusClientMap.entrySet()) {
                try {
                    callables.add(new BackupOperationCallable(requestPreparation.prepare(entry.getValue(), globalOperation.request, topology),
                                                              entry.getValue(),
                                                              progressTracker));
                } catch (final OperationCoordinatorException e) {
                    resultGatherer.gather(globalOperation, e);
                    return resultGatherer;
                }
            }

            // submit & gather results

            allOf(callables.stream().map(c -> supplyAsync(c, executorService).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    throwable.printStackTrace();
                    logger.warn(format("Backup against %s has failed. ", result.request.storageLocation));
                    resultGatherer.gather(result, throwable);
                }

                try {
                    result.close();
                } catch (final Exception ex) {
                    logger.error(format("Unable to close a sidecar client for operation %s upon gathering results from other sidecars: %s", result.id, ex.getMessage()));
                }
            })).toArray(CompletableFuture<?>[]::new)).get();
        } catch (ExecutionException | InterruptedException ex) {
            ex.printStackTrace();
            resultGatherer.gather(globalOperation, new OperationCoordinatorException("Unable to coordinate backup! " + ex.getMessage(), ex));
        } finally {
            executorService.shutdownNow();
        }

        return resultGatherer;
    }

    private static class BackupOperationCallable extends OperationCallable<BackupOperation, BackupOperationRequest> {

        public BackupOperationCallable(final Operation<BackupOperationRequest> operation,
                                       final IcarusClient icarusClient,
                                       final GlobalOperationProgressTracker progressTracker) {
            super(operation, operation.request.timeout, icarusClient, progressTracker, "backup");
        }

        public OperationResult<BackupOperation> sendOperation() {
            return super.icarusClient.backup(super.operation.request);
        }
    }
}
