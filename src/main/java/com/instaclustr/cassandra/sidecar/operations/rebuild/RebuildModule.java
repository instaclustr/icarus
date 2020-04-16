package com.instaclustr.cassandra.sidecar.operations.rebuild;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class RebuildModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "rebuild",
                                 RebuildOperationRequest.class,
                                 RebuildOperation.class);
    }
}
