package com.instaclustr.cassandra.sidecar.operations.drain;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class DrainModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "drain",
                                 DrainOperationRequest.class,
                                 DrainOperation.class);
    }
}
