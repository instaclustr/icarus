package com.instaclustr.cassandra.sidecar.operations.restart;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class RestartModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "restart",
                                 RestartOperationRequest.class,
                                 RestartOperation.class);

        installOperationBindings(binder(),
                                 "restart-sidecar",
                                 RestartSidecarOperationRequest.class,
                                 RestartSidecarOperation.class);
    }
}
