package com.instaclustr.icarus.operations.icarus;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import com.google.inject.AbstractModule;

public class IcarusModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "stop",
                                 StopIcarusOperationRequest.class,
                                 StopIcarusOperation.class);
    }
}
