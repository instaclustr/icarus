package com.instaclustr.icarus.coordination;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.icarus.coordination.CoordinationUtils.constructSidecars;
import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.instaclustr.esop.guice.BucketServiceFactory;
import com.instaclustr.esop.guice.RestorerFactory;
import com.instaclustr.esop.impl.BucketService;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType;
import com.instaclustr.esop.impl.restore.RestorationStrategyResolver;
import com.instaclustr.esop.impl.restore.RestoreOperation;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.restore.coordination.BaseRestoreOperationCoordinator;
import com.instaclustr.esop.topology.CassandraClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.operations.GlobalOperationProgressTracker;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import com.instaclustr.threading.Executors.ExecutorServiceSupplier;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcarusRestoreOperationCoordinator extends BaseRestoreOperationCoordinator {

    private static final Logger logger = LoggerFactory.getLogger(IcarusBackupOperationCoordinator.class);

    private final CassandraJMXService cassandraJMXService;
    private final SidecarSpec icarusSpec;

    private final ExecutorServiceSupplier executorServiceSupplier;
    private final OperationsService operationsService;
    private final ObjectMapper objectMapper;
    private final Map<String, BucketServiceFactory> bucketServiceFactoryMap;

    @Inject
    public IcarusRestoreOperationCoordinator(final Map<String, RestorerFactory> restorerFactoryMap,
                                             final RestorationStrategyResolver restorationStrategyResolver,
                                             final CassandraJMXService cassandraJMXService,
                                             final SidecarSpec icarusSpec,
                                             final ExecutorServiceSupplier executorServiceSupplier,
                                             final OperationsService operationsService,
                                             final ObjectMapper objectMapper,
                                             final Map<String, BucketServiceFactory> bucketServiceFactoryMap) {
        super(restorerFactoryMap, restorationStrategyResolver);
        this.cassandraJMXService = cassandraJMXService;
        this.icarusSpec = icarusSpec;
        this.executorServiceSupplier = executorServiceSupplier;
        this.operationsService = operationsService;
        this.objectMapper = objectMapper;
        this.bucketServiceFactoryMap = bucketServiceFactoryMap;
    }

    @Override
    public void coordinate(final Operation<RestoreOperationRequest> operation) throws OperationCoordinatorException {

        try (final BucketService bucketService = bucketServiceFactoryMap.get(operation.request.storageLocation.storageProvider).createBucketService(operation.request)) {
            bucketService.checkBucket(operation.request.storageLocation.bucket, false);
        } catch (final Exception ex) {
            throw new OperationCoordinatorException(format("Bucket %s does not exist or we could not determine its existence.",
                                                           operation.request.storageLocation.bucket), ex);
        }

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

            final List<UUID> restoreUUIDs = operationsService.allRunningOfType("restore");

            if (restoreUUIDs.size() > 2) {
                throw new IllegalStateException("There are more than two concurrent restore operations running!");
            }

            int normalRequests = 0;

            for (final UUID uuid : restoreUUIDs) {
                final Optional<Operation> operationOptional = operationsService.operation(uuid);

                if (!operationOptional.isPresent()) {
                    throw new IllegalStateException(format("received empty optional for uuid %s", uuid.toString()));
                }

                final Operation op = operationOptional.get();

                if (!(op.request instanceof RestoreOperationRequest)) {
                    throw new IllegalStateException(format("Received request is not of type %s", RestoreOperationRequest.class));
                }

                RestoreOperationRequest request = (RestoreOperationRequest) op.request;

                if (!request.globalRequest) {
                    normalRequests += 1;
                }
            }

            if (normalRequests == 2) {
                throw new IllegalStateException("We can not run two normal restoration requests simultaneously.");
            }

            super.coordinate(operation);
            return;
        }

        // if it is a global request, we will coordinate whole restore across a cluster in this operation
        // when this operation finishes, whole cluster will be restored.

        // first we have to make some basic checks, e.g. we can be the only global restore operation on this node
        // and no other restore operations (even partial) can run simultaneously

        final List<UUID> restoreUUIDs = operationsService.allRunningOfType("restore");

        if (restoreUUIDs.size() != 1) {
            throw new IllegalStateException("There is more than one running restoration operation.");
        }

        if (!restoreUUIDs.get(0).equals(operation.id)) {
            throw new IllegalStateException("ID of a running operation does not equal to ID of this restore operation!");
        }

        if (operation.request.restorationPhase != DOWNLOAD) {
            throw new IllegalStateException(format("Restoration coordination has to start with %s phase.", DOWNLOAD));
        }

        try (final IcarusWrapper icarusWrapper = new IcarusWrapper(getSidecarClients())) {
            final IcarusWrapper oneClient = getOneClient(icarusWrapper);

            final ResultSupplier[] resultSuppliers = new ResultSupplier[]{
                    () -> executePhase(new InitPhasePreparation(), operation, oneClient),
                    () -> executePhase(new DownloadPhasePreparation(), operation, icarusWrapper),
                    () -> executePhase(new TruncatePhasePreparation(), operation, oneClient),
                    () -> executePhase(new ImportingPhasePreparation(), operation, icarusWrapper),
                    () -> executePhase(new CleaningPhasePreparation(), operation, icarusWrapper),
            };

            for (final ResultSupplier supplier : resultSuppliers) {
                supplier.getWithEx();

                if (operation.hasErrors()) {
                    break;
                }
            }
        } catch (final Exception ex) {
            operation.addError(Operation.Error.from(new OperationCoordinatorException("Unable to coordinate distributed restore.", ex)));
        }
    }

    public static class IcarusWrapper implements Closeable {
        public Map<InetAddress, IcarusClient> icarusClients;

        public IcarusWrapper(final Map<InetAddress, IcarusClient> icarusClients) {
            this.icarusClients = icarusClients;
        }

        @Override
        public void close() {
            if (icarusClients != null) {
                for (final IcarusClient icarusClient : icarusClients.values()) {
                    if (icarusClient != null) {
                        try {
                            icarusClient.close();
                        } catch (final Exception ex) {
                            logger.error("Unable to close the client {}", icarusClient);
                        }
                    }
                }
            }
        }
    }

    @FunctionalInterface
    private interface ResultSupplier {

        void getWithEx() throws Exception;
    }

    private IcarusWrapper getOneClient(final IcarusWrapper icarusWrapper) {
        if (icarusWrapper.icarusClients != null) {
            final Iterator<Entry<InetAddress, IcarusClient>> it = icarusWrapper.icarusClients.entrySet().iterator();

            if (it.hasNext()) {
                final Entry<InetAddress, IcarusClient> next = it.next();
                return new IcarusWrapper(new HashMap<InetAddress, IcarusClient>() {{
                    put(next.getKey(), next.getValue());
                }});
            }
        }

        throw new IllegalStateException("Unable to detect what client belong to this node!");
    }

    private static abstract class PhasePreparation {

        Operation<RestoreOperationRequest> prepare(final IcarusClient client, final RestoreOperationRequest request) throws OperationCoordinatorException {
            try {
                final RestoreOperation restoreOperation = cloneOp(request);
                prepareBasics(restoreOperation.request, client);
                restoreOperation.request.restorationPhase = getPhaseType();

                return restoreOperation;
            } catch (final Exception ex) {
                throw new OperationCoordinatorException(format("Unable to prepare operation for %s phase.", getPhaseType()), ex);
            }
        }

        abstract RestorationPhaseType getPhaseType();

        RestoreOperation cloneOp(final RestoreOperationRequest request) throws CloneNotSupportedException {
            final RestoreOperationRequest clonedRequest = (RestoreOperationRequest) request.clone();
            return new RestoreOperation(clonedRequest);
        }

        void prepareBasics(final RestoreOperationRequest request, final IcarusClient client) throws OperationCoordinatorException {

            if (!client.getHostId().isPresent()) {
                throw new OperationCoordinatorException(format("There is not any hostId for client %s", client.getHost()));
            }

            request.storageLocation = StorageLocation.update(request.storageLocation, client.getClusterName(), client.getDc(), client.getHostId().get().toString());
            request.storageLocation.globalRequest = false;
            request.globalRequest = false;
        }
    }

    private static final class DownloadPhasePreparation extends PhasePreparation {
        @Override
        public RestorationPhaseType getPhaseType() {
            return DOWNLOAD;
        }
    }

    private static final class TruncatePhasePreparation extends PhasePreparation {
        @Override
        public RestorationPhaseType getPhaseType() {
            return TRUNCATE;
        }
    }

    private static final class ImportingPhasePreparation extends PhasePreparation {
        @Override
        public RestorationPhaseType getPhaseType() {
            return IMPORT;
        }
    }

    private static final class CleaningPhasePreparation extends PhasePreparation {
        @Override
        public RestorationPhaseType getPhaseType() {
            return CLEANUP;
        }
    }

    private static final class InitPhasePreparation extends PhasePreparation {
        @Override
        RestorationPhaseType getPhaseType() {
            return INIT;
        }
    }

    private Map<InetAddress, IcarusClient> getSidecarClients() throws Exception {
        final ClusterTopology clusterTopology = new CassandraClusterTopology(cassandraJMXService, null).act();
        return constructSidecars(clusterTopology.clusterName, clusterTopology.endpoints, clusterTopology.endpointDcs, icarusSpec, objectMapper);
    }

    private void executePhase(final PhasePreparation phasePreparation,
                              final Operation<RestoreOperationRequest> globalOperation,
                              final IcarusWrapper icarusWrapper) throws OperationCoordinatorException {
        final ExecutorService executorService = executorServiceSupplier.get(MAX_NUMBER_OF_CONCURRENT_OPERATIONS);

        try {
            final List<RestoreOperationCallable> callables = new ArrayList<>();
            final GlobalOperationProgressTracker progressTracker = new GlobalOperationProgressTracker(globalOperation, icarusWrapper.icarusClients.entrySet().size());

            // create

            for (final Entry<InetAddress, IcarusClient> entry : icarusWrapper.icarusClients.entrySet()) {
                callables.add(new RestoreOperationCallable(phasePreparation.prepare(entry.getValue(), globalOperation.request),
                                                           entry.getValue(),
                                                           progressTracker));
            }

            // submit & gather results

            allOf(callables.stream().map(c -> supplyAsync(c, executorService).whenComplete((result, throwable) -> {
                if (throwable != null) {
                    throwable.printStackTrace();
                    logger.warn(format("Restore from %s has failed. ", result.request.storageLocation));
                    globalOperation.addError(Operation.Error.from(throwable));
                } else {
                    // add errors if any, the fact that throwable is null does not neccessarilly mean that underlaying operation has not failed.
                    globalOperation.addErrors(result.errors);
                }

                try {
                    result.close();
                } catch (final Exception ex) {
                    logger.error(format("Unable to close a sidecar client for operation %s upon gathering results from other sidecars: %s", result.id, ex.getMessage()));
                }
            })).toArray(CompletableFuture<?>[]::new)).get();

        } catch (ExecutionException | InterruptedException ex) {
            globalOperation.addError(Operation.Error.from(new OperationCoordinatorException("Unable to coordinate restoration!", ex)));
        } finally {
            executorService.shutdownNow();
        }
    }

    private static class RestoreOperationCallable extends OperationCallable<RestoreOperation, RestoreOperationRequest> {

        public RestoreOperationCallable(final Operation<RestoreOperationRequest> operation,
                                        final IcarusClient icarusClient,
                                        final GlobalOperationProgressTracker progressTracker) {
            super(operation, operation.request.timeout, icarusClient, progressTracker, operation.request.restorationPhase.toString().toLowerCase());
        }

        public IcarusClient.OperationResult<RestoreOperation> sendOperation() {
            return icarusClient.restore(operation.request);
        }
    }
}