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
                    final State returnedState = operation.state;

                    float delta = operation.progress - floatAtomicReference.get();
                    floatAtomicReference.set(floatAtomicReference.get() + delta);

                    progressTracker.update(delta);

                    if (FAILED == returnedState) {
                        throw new OperationCoordinatorException(format("Operation %s of type %s in phase %s against host %s has failed.",
                                                                       operation.id,
                                                                       operation.request.type,
                                                                       phase,
                                                                       icarusClient.getHost()));
                    }

                    return State.TERMINAL_STATES.contains(returnedState);
                }

                throw new OperationCoordinatorException(format("Error while fetching state of operation %s of type %s in phase %s against host %s, returned code: %s",
                                                               operation.id,
                                                               operation.request.type,
                                                               phase,
                                                               icarusClient.getHost(),
                                                               operationResult.response.getStatus()));
            } catch (final Exception ex) {
                progressTracker.complete();
                operation.failureCause = null;
                operation.state = FAILED;
                operation.completionTime = Instant.now();
                operation.failureCause = ex;
                throw ex;
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
