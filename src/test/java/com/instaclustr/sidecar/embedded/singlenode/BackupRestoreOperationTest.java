package com.instaclustr.sidecar.embedded.singlenode;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;

import java.io.File;
import java.nio.file.Path;

import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient.OperationResult;
import com.instaclustr.io.FileUtils;
import com.instaclustr.operations.OperationsService;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import org.testng.annotations.Test;

public class BackupRestoreOperationTest extends AbstractCassandraSidecarTest {

    @Test
    public void backupTest() throws Exception {

        try {
            final SidecarHolder sidecarHolder = sidecars.get("datacenter1");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest(
                    "backup",
                    new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    cassandraDir.resolve("data"),
                    DatabaseEntities.parse("system_schema," + keyspaceName),
                    "stefansnapshot",
                    "default",
                    "test-sidecar-secret",
                    false,
                    null,
                    false,
                    null // timeout
            );

            final OperationResult<BackupOperation> result = sidecarHolder.sidecarClient.backup(backupOperationRequest);

            sidecarHolder.sidecarClient.waitForCompleted(result);

            final OperationsService operationService = sidecarHolder.injector.getInstance(OperationsService.class);

            sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest(
                    "restore", // type
                    new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1"), // storage location
                    null, // concurrent connections
                    null, // lock file
                    cassandraDir.resolve("data"), // cassandra dir
                    cassandraDir.resolve("config"), // cassandra config dir
                    false, // restore system keyspace
                    "stefansnapshot", // snapshot
                    DatabaseEntities.parse(keyspaceName), // entities
                    false, // update cassandra yaml
                    HARDLINKS, // restoration strategy
                    DOWNLOAD, // restoration phase
                    new ImportOperationRequest(null, null, downloadDir), // import
                    false, // no delete truncates
                    false, // no delete data
                    false, // no download data
                    false, // exact schema version
                    null, // schema version
                    null, // k8s namespace
                    null, // k8s secret name
                    false, // NO GLOBAL REQUEST
                    null, // timeout
                    false // resolveHostIdFromTopology
            );

            sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));

            restoreOperationRequest.restorationPhase = TRUNCATE;
            sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));

            restoreOperationRequest.restorationPhase = IMPORT;
            sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));
            restoreOperationRequest.restorationPhase = CLEANUP;

            sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));

            dbHelper.dump(keyspaceName, tableName);
        } catch (final Exception ex) {
            FileUtils.deleteDirectory(cassandraDir);
            FileUtils.deleteDirectory(downloadDir);
        }
    }

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path downloadDir = new File("target/downloaded").toPath().toAbsolutePath();

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

}
