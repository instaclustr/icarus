package com.instaclustr.icarus.coordination;

import static com.instaclustr.operations.Operation.State.FAILED;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.Closeable;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.rest.IcarusClient.OperationResult;
import com.instaclustr.operations.GlobalOperationProgressTracker;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.Operation.State;
import com.instaclustr.operations.OperationCoordinator.OperationCoordinatorException;
import com.instaclustr.operations.OperationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OperationCallable<O extends Operation<T>, T extends OperationRequest> implements Supplier<Operation<T>>, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(OperationCallable.class);

    protected Operation<T> operation;
    protected final int timeout;
    protected final IcarusClient icarusClient;
    private final GlobalOperationProgressTracker progressTracker;
    private final String phase;

    /**
     * @param operation       operation to execute
     * @param timeout         time to pass until an operation is in terminal state to not consider it to be failed
     * @param icarusClient    client to execute this operation wit
     * @param progressTracker progress tracker
     * @param phase           phase to execute this operation against
     *                        <p>
     *                        In case a a progress tracker is shared among multiple phases, it is required that the number of operations for that tracker
     *                        is equal to the sum of all partial operations in each phase. Each terminal state of an operation will update tracker by 1.0f.
     */
    public OperationCallable(final Operation<T> operation,
                             final int timeout,
                             final IcarusClient icarusClient,
                             final GlobalOperationProgressTracker progressTracker,
                             final String phase) {
        this.operation = operation;
        this.icarusClient = icarusClient;
        this.progressTracker = progressTracker;
        this.phase = phase;
        this.timeout = timeout;
    }

    public abstract OperationResult<O> sendOperation();

    @Override
    public Operation<T> get() {
        logger.info(format("Submitting operation %s with request %s ",
                           operation.getClass().getCanonicalName(),
                           operation.request.toString()));

        final OperationResult<O> operationResult = sendOperation();

        logger.info(format("Sent %s operation in phase %s to node %s", operation.type, phase, icarusClient.getHost()));

        final AtomicReference<Float> floatAtomicReference = new AtomicReference<>((float) 0);

        await().timeout(timeout, HOURS).pollInterval(5, SECONDS).until(() -> {

            try {
                if (operationResult.operation != null) {
                    operation = icarusClient.getOperation(operationResult.operation.id, operation.request);

                    // even an operation returns here ok but its status if FAILED,
                    // we do still have a progress of such operation updated to 100%
                    // so we need to update the global progress tracked by remaining progress

                    float delta = operation.progress - floatAtomicReference.get();
                    floatAtomicReference.accumulateAndGet(delta, Float::sum);

                    progressTracker.update(delta);

                    return State.TERMINAL_STATES.contains(operation.state);
                }

                throw new OperationCoordinatorException(format("Error while fetching state of operation %s of type %s in phase %s against host %s, returned code: %s",
                                                               operation.id,
                                                               operation.request.type,
                                                               phase,
                                                               icarusClient.getHost(),
                                                               operationResult.response.getStatus()));
            } catch (final Exception ex) {
                // this is reached only in case the response itself can not be fetched
                // if that response itself is returned but that remote operation failed,
                // it would be treated in try block and returned from there
                operation.state = FAILED;
                operation.completionTime = Instant.now();

                operation.addError(Operation.Error.from(icarusClient.getHost(), ex));

                // consider this operation to be finished when it failed
                // if an operation is finished on 80% and it fails, reference was
                // never updated so we add the remaining progress subtracting it from 100% (1.0)
                progressTracker.update(1.0f - floatAtomicReference.get());

                return true;
            }
        });

        final String logMessage = format("operation %s against node %s with hostId %s has finished with state %s.",
                                         operation.id,
                                         icarusClient.getHost(),
                                         icarusClient.getHostId(),
                                         operation.state);

        if (operation.state == FAILED) {
            logger.error(logMessage);
        } else {
            logger.info(logMessage);
        }

        return operation;
    }

    @Override
    public void close() {
        try {
            icarusClient.close();
        } catch (final Exception ex) {
            logger.error(format("Unable to close callable for %s", icarusClient.getHost()));
        }
    }
}
