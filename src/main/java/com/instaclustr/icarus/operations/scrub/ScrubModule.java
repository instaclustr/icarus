package com.instaclustr.icarus.operations.scrub;

import com.google.inject.AbstractModule;
import com.instaclustr.operations.OperationBindings;

public class ScrubModule extends AbstractModule {
    @Override
    protected void configure() {
        OperationBindings.installOperationBindings(binder(),
                                                   "scrub",
                                                   ScrubOperationRequest.class,
                                                   ScrubOperation.class);
    }
}
