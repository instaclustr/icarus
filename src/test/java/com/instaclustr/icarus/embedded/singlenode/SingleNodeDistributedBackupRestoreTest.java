package com.instaclustr.icarus.embedded.singlenode;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.measure.DataRate;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import org.testng.annotations.Test;

/**
 * This tests invariant - what happens when we are doing distributed backup / restore on a cluster consisting of just one node.
 * It means that it will send these phase requests to itself only.
 */
public class SingleNodeDistributedBackupRestoreTest extends AbstractCassandraIcarusTest {

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
                false, // skip bucket verification
                null, // schema version
                false, // topology file, even it is false, global request does not care, it will upload it anyway
                null // proxy
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
                false, // newCluster
                false, // skip bucket verification
                null // proxy
        );
    }

    @Test
    public void backupTest() {

        final SidecarHolder sidecarHolder = sidecars.get("datacenter1");

        BackupOperationRequest backupOperationRequest1 = createBackupRequest("stefansnapshot");
        BackupOperationRequest backupOperationRequest2 = createBackupRequest("stefansnapshot2");

        // two concurrent backups
        IcarusClient.OperationResult<BackupOperation> backup1 = sidecarHolder.icarusClient.backup(backupOperationRequest1);
        IcarusClient.OperationResult<BackupOperation> backup2 = sidecarHolder.icarusClient.backup(backupOperationRequest2);

        sidecarHolder.icarusClient.waitForCompleted(backup1);
        sidecarHolder.icarusClient.waitForCompleted(backup2);

        sidecarHolder.icarusClient.waitForCompleted(sidecarHolder.icarusClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

        RestoreOperationRequest restoreRequest = createRestoreOperationRequest(sidecarHolder.icarusClient.getCassandraSchemaVersion().getSchemaVersion());

        sidecarHolder.icarusClient.waitForCompleted(sidecarHolder.icarusClient.restore(restoreRequest));

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
