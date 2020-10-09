package com.instaclustr.icarus.operations.sidecar;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class SidecarModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "stop",
                                 StopSidecarOperationRequest.class,
                                 StopSidecarOperation.class);
    }
}
