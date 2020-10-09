package com.instaclustr.icarus.operations.sidecar;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import javax.inject.Inject;
import java.util.List;

import com.google.inject.assistedinject.Assisted;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In context of Kubernetes, exiting a process will kill a container and it will be restarted.
 */
public class StopSidecarOperation extends Operation<StopSidecarOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(StopSidecarOperation.class);

    private final OperationsService operationsService;

    @Inject
    public StopSidecarOperation(OperationsService operationsService,
                                @Assisted final StopSidecarOperationRequest request) {
        super(request);
        this.operationsService = operationsService;
    }

    @Override
    protected void run0() throws Exception {
        Awaitility.await().pollInterval(10, SECONDS).atMost(5, MINUTES).until(() -> {
            final List<Operation> otherOperations = operationsService.getOperations(op -> op.request.getClass() != StopSidecarOperationRequest.class);

            if (otherOperations.size() != 0) {
                logger.info("Waiting for operations to stop:  " + otherOperations.stream().map(op -> op.id).collect(toList()));
                return false;
            }

            return true;
        });

        logger.info("Stopping OperationService ...");

        operationsService.stopAsync();

        logger.info("Awaiting termination of OperationService ...");

        operationsService.awaitTerminated();

        logger.info("Killing sidecar container.");
        System.exit(0);
    }
}
