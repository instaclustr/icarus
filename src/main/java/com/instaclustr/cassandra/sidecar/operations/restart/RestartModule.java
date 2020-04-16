package com.instaclustr.cassandra.sidecar.operations.restart;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class RestartModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "restart",
                                 RestartOperationRequest.class,
                                 RestartOperation.class);
    }
}
