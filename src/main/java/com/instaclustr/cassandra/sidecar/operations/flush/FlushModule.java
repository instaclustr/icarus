package com.instaclustr.cassandra.sidecar.operations.flush;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class FlushModule extends AbstractModule {

    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "flush",
                                 FlushOperationRequest.class,
                                 FlushOperation.class);
    }
}
