package com.instaclustr.cassandra.sidecar.operations.restart;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import javax.inject.Inject;

import com.google.inject.assistedinject.Assisted;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationsService;
import org.awaitility.Awaitility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In context of Kubernetes, exiting a process will kill a container and it will be restarted.
 */
public class RestartSidecarOperation extends Operation<RestartSidecarOperationRequest> {

    private static final Logger logger = LoggerFactory.getLogger(RestartSidecarOperation.class);

    private final OperationsService operationsService;

    @Inject
    public RestartSidecarOperation(OperationsService operationsService,
                                   @Assisted final RestartSidecarOperationRequest request) {
        super(request);
        this.operationsService = operationsService;
    }

    @Override
    protected void run0() throws Exception {
        Awaitility.await().pollInterval(5, SECONDS).atMost(5, MINUTES).until(() -> {
            // only ours restart operation is running
            return operationsService.allRunningOfType("restart-sidecar").size() == 1;
        });

        logger.info("Stopping OperationService ...");

        operationsService.stopAsync();

        logger.info("Awaiting termination of OperationService ...");

        operationsService.awaitTerminated();

        logger.info("Killing sidecar container.");
        System.exit(0);
    }
}
