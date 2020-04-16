package com.instaclustr.cassandra.sidecar.operations.refresh;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class RefreshModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "refresh",
                                 RefreshOperationRequest.class,
                                 RefreshOperation.class);
    }
}
