package com.instaclustr.cassandra.sidecar.operations.upgradesstables;

import com.google.inject.AbstractModule;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;

public class UpgradeSSTablesModule extends AbstractModule {
    @Override
    protected void configure() {
        installOperationBindings(binder(),
                                 "upgradesstables",
                                 UpgradeSSTablesOperationRequest.class,
                                 UpgradeSSTablesOperation.class);
    }
}
