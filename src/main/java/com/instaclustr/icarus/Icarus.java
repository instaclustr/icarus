package com.instaclustr.icarus;

import static com.google.inject.Guice.createInjector;
import static com.google.inject.Stage.PRODUCTION;
import static com.instaclustr.operations.OperationBindings.installOperationBindings;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.instaclustr.cassandra.CassandraModule;
import com.instaclustr.esop.guice.StorageModules;
import com.instaclustr.esop.impl._import.ImportModule;
import com.instaclustr.esop.impl.backup.BackupModules;
import com.instaclustr.esop.impl.backup.BackupModules.BackupModule;
import com.instaclustr.esop.impl.backup.BackupModules.CommitlogBackupModule;
import com.instaclustr.esop.impl.hash.HashModule;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.esop.impl.list.ListModule;
import com.instaclustr.esop.impl.remove.RemoveBackupModule;
import com.instaclustr.esop.impl.restore.RestoreModules;
import com.instaclustr.esop.impl.restore.RestoreModules.RestorationStrategyModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreCommitlogModule;
import com.instaclustr.esop.impl.restore.RestoreModules.RestoreModule;
import com.instaclustr.esop.impl.truncate.TruncateOperation;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.guice.Application;
import com.instaclustr.guice.ServiceManagerModule;
import com.instaclustr.icarus.operations.cleanup.CleanupsModule;
import com.instaclustr.icarus.operations.decommission.DecommissioningModule;
import com.instaclustr.icarus.operations.drain.DrainModule;
import com.instaclustr.icarus.operations.flush.FlushModule;
import com.instaclustr.icarus.operations.rebuild.RebuildModule;
import com.instaclustr.icarus.operations.refresh.RefreshModule;
import com.instaclustr.icarus.operations.scrub.ScrubModule;
import com.instaclustr.icarus.operations.icarus.IcarusModule;
import com.instaclustr.icarus.operations.upgradesstables.UpgradeSSTablesModule;
import com.instaclustr.icarus.service.ServicesModule;
import com.instaclustr.jackson.JacksonModule;
import com.instaclustr.operations.OperationsModule;
import com.instaclustr.picocli.CLIApplication;
import com.instaclustr.picocli.CassandraJMXSpec;
import com.instaclustr.sidecar.http.JerseyHttpServerModule;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import com.instaclustr.threading.ExecutorsModule;
import com.instaclustr.version.VersionModule;
import jmx.org.apache.cassandra.CassandraJMXConnectionInfo;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

@Command(name = "icarus",
         description = "Sidecar management application for Cassandra.",
         versionProvider = Icarus.class,
         sortOptions = false,
         usageHelpWidth = 128,
         mixinStandardHelpOptions = true
)
public final class Icarus extends CLIApplication implements Callable<Void> {

    @Mixin
    public SidecarSpec icarusSpec;

    @Mixin
    public CassandraJMXSpec jmxSpec;

    @Mixin
    public HashSpec hashSpec;

    @Spec
    private CommandSpec commandSpec;

    @Option(names = {"--enable-truncate"},
            description = "If enabled, Icarus will expose truncate operation on REST, defaults to false.")
    private boolean enableTruncateOperation;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void mainWithoutExit(String[] args) {
        main(args, false);
    }

    public static void main(final String[] args, boolean exit) {
        int exitCode = execute(new Icarus(), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public Void call() throws Exception {
        // production binds singletons as eager by default
        final Injector injector = createInjector(PRODUCTION, getModules(icarusSpec, jmxSpec, hashSpec, enableTruncateOperation));

        return injector.getInstance(Application.class).call();
    }

    public List<AbstractModule> getModules(final SidecarSpec icarusSpec,
                                           final CassandraJMXSpec jmxSpec,
                                           final HashSpec hashSpec,
                                           final boolean enableTruncateOperation) throws Exception {
        List<AbstractModule> modules = new ArrayList<>();

        modules.addAll(backupRestoreModules(hashSpec));
        modules.addAll(operationModules());
        modules.addAll(icarusModules(icarusSpec, jmxSpec));

        if (enableTruncateOperation) {
            modules.add(new AbstractModule() {
                @Override
                protected void configure() {
                    installOperationBindings(binder(),
                                             "truncate",
                                             TruncateOperationRequest.class,
                                             TruncateOperation.class);
                }
            });
        }

        return modules;
    }

    public List<AbstractModule> icarusModules(SidecarSpec icarusSpec, CassandraJMXSpec jmxSpec) throws Exception {
        return new ArrayList<AbstractModule>() {{
            add(new VersionModule(getVersion()));
            add(new ServiceManagerModule());
            add(new CassandraModule(new CassandraJMXConnectionInfo(jmxSpec.jmxPassword,
                                                                   jmxSpec.jmxUser,
                                                                   jmxSpec.jmxCredentials,
                                                                   jmxSpec.jmxServiceURL,
                                                                   jmxSpec.trustStore,
                                                                   jmxSpec.trustStorePassword,
                                                                   jmxSpec.keyStore,
                                                                   jmxSpec.keyStorePassword,
                                                                   jmxSpec.jmxClientAuth)));
            add(new JerseyHttpServerModule(icarusSpec.httpServerAddress, icarusSpec.disableCors));
            add(new OperationsModule(icarusSpec.operationsExpirationPeriod));
            add(new ExecutorsModule());
            add(new JacksonModule());
            add(new ServicesModule());
            add(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(SidecarSpec.class).toInstance(icarusSpec);
                }
            });
        }};
    }

    public static List<AbstractModule> backupRestoreModules(final HashSpec hashSpec) {
        return new ArrayList<AbstractModule>() {{
            add(new StorageModules());
            add(new BackupModule());
            add(new CommitlogBackupModule());
            add(new RestoreModule());
            add(new RestoreCommitlogModule());
            add(new RestorationStrategyModule());
            add(new BackupModules.UploadingModule());
            add(new RestoreModules.DownloadingModule());
            add(new HashModule(hashSpec));
            add(new ListModule());
            add(new RemoveBackupModule());
        }};
    }

    public static List<AbstractModule> operationModules() {
        return new ArrayList<AbstractModule>() {{
            add(new DecommissioningModule());
            add(new CleanupsModule());
            add(new UpgradeSSTablesModule());
            add(new RebuildModule());
            add(new ScrubModule());
            add(new DrainModule());
            add(new IcarusModule());
            add(new RefreshModule());
            add(new FlushModule());
            add(new ImportModule());
        }};
    }

    @Override
    public String title() {
        return "instaclustr-icarus";
    }
}
