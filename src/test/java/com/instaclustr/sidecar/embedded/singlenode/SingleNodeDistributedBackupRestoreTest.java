package com.instaclustr.sidecar.embedded.singlenode;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.instaclustr.cassandra.backup.impl.DatabaseEntities;
import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl._import.ImportOperationRequest;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
import com.instaclustr.measure.DataRate;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import org.testng.annotations.Test;

/**
 * This tests invariant - what happens when we are doing distributed backup / restore on a cluster consisting of just one node.
 * It means that it will send these phase requests to itself only.
 */
public class SingleNodeDistributedBackupRestoreTest extends AbstractCassandraSidecarTest {

    private BackupOperationRequest createBackupRequest(String snapshotName) {
        return new BackupOperationRequest(
                "backup", // type
                new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/global"), // storage
                null, // duration
                new DataRate(1L, DataRate.DataRateUnit.MBPS), // bandwidth
                15, // concurrentConnections
                null, // metadata
                cassandraDir.resolve("data"), // cassandra dir
                DatabaseEntities.parse("system_schema," + keyspaceName), // entities
                snapshotName, // snapshot
                "default", // k8s namespace
                "test-sidecar-secret", // k8s secret
                true, // !!! GLOBAL REQUEST !!!
                null, // DC is null so will backup all datacenters
                null, // timeout
                false, // insecure
                true, // create bucket when missing
                null // schema version
        );
    }

    private RestoreOperationRequest createRestoreOperationRequest(String schemaVersion) {
        return new RestoreOperationRequest(
                "restore", // type
                new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/global"), // storage location
                null, // concurrent connections
                null, // lock file
                cassandraDir.resolve("data"), // cassandra dir
                cassandraDir.resolve("config"), // cassandra config dir
                false, // restore system keyspace
                "stefansnapshot-" + schemaVersion, // snapshot + schema version, it does not need to be there, just for testing purposes
                DatabaseEntities.parse(keyspaceName), // entities
                false, // update cassandra yaml
                HARDLINKS, // restoration strategy
                DOWNLOAD, // restoration phase
                new ImportOperationRequest(null, null, downloadDir), // import
                false, // noDeleteTruncates
                false, // noDeleteDownload
                false, // noDownloadData
                false, // exactSchemaVersion
                null, // schema version
                null, // k8s namespace
                null, // k8s secret name
                true, // !!! GLOBAL REQUEST !!!
                null, // timeout,
                false, // resolve topology
                false, // insecure
                false // newCluster
        );
    }

    @Test
    public void backupTest() {

        final SidecarHolder sidecarHolder = sidecars.get("datacenter1");

        BackupOperationRequest backupOperationRequest1 = createBackupRequest("stefansnapshot");
        BackupOperationRequest backupOperationRequest2 = createBackupRequest("stefansnapshot2");

        // two concurrent backups
        SidecarClient.OperationResult<BackupOperation> backup1 = sidecarHolder.sidecarClient.backup(backupOperationRequest1);
        SidecarClient.OperationResult<BackupOperation> backup2 = sidecarHolder.sidecarClient.backup(backupOperationRequest2);

        sidecarHolder.sidecarClient.waitForCompleted(backup1);
        sidecarHolder.sidecarClient.waitForCompleted(backup2);

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

        RestoreOperationRequest restoreRequest = createRestoreOperationRequest(sidecarHolder.sidecarClient.getCassandraSchemaVersion().getSchemaVersion());

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreRequest));

        dbHelper.dump(keyspaceName, tableName);
    }

    @Override
    public void customAfterClass() throws Exception {
        deleteDirectory(cassandraDir);
        deleteDirectory(downloadDir);
        deleteDirectory(backupDir);
    }

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path downloadDir = new File("target/downloaded").toPath().toAbsolutePath();
    protected final Path backupDir = Paths.get(target("backup1"));

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

}
