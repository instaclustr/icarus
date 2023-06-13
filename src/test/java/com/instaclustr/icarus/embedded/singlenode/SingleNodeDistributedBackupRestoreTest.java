package com.instaclustr.icarus.embedded.singlenode;

import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.measure.DataRate;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.INIT;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;
import static com.instaclustr.io.FileUtils.deleteDirectory;

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
                DatabaseEntities.parse(keyspaceName), //DatabaseEntities.parse("system_schema," + keyspaceName), // entities
                snapshotName, // snapshot
                true, // !!! GLOBAL REQUEST !!!
                null, // DC is null so will backup all datacenters
                null, // timeout
                false, // insecure
                true, // create bucket when missing
                false, // skip bucket verification
                null, // schema version
                false, // topology file, even it is false, global request does not care, it will upload it anyway
                null, // proxy
                null, // retry
                false, // skip refreshing
                dataDirs, // cassandra dir
                null // kms
        );
    }

    private RestoreOperationRequest createRestoreOperationRequest(String schemaVersion) {
        return new RestoreOperationRequest(
                "restore", // type
                new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/global"), // storage location
                null, // concurrent connections
                cassandraDir.resolve("data"), // cassandra dir
                cassandraDir.resolve("config"), // cassandra config dir
                false, // restore system keyspace
                false, // restore system auth keyspace
                "stefansnapshot-" + schemaVersion, // snapshot + schema version, it does not need to be there, just for testing purposes
                DatabaseEntities.parse(keyspaceName), // entities
                false, // update cassandra yaml
                HARDLINKS, // restoration strategy
                INIT, // restoration phase
                new ImportOperationRequest(null, null, downloadDir), // import
                false, // noDeleteTruncates
                false, // noDeleteDownload
                false, // noDownloadData
                false, // exactSchemaVersion
                null, // schema version
                true, // !!! GLOBAL REQUEST !!!
                null, // dc
                null, // timeout,
                false, // resolve topology
                false, // insecure
                false, // newCluster
                false, // skip bucket verification
                null, // proxy
                null, // rename
                null,
                false,
                dataDirs,
                null
        );
    }

    @Test
    public void backupTest() {

        final IcarusHolder icarusHolder = icaruses.get("datacenter1");

        BackupOperationRequest backupOperationRequest1 = createBackupRequest("stefansnapshot");
        BackupOperationRequest backupOperationRequest2 = createBackupRequest("stefansnapshot2");

        // two concurrent backups
        IcarusClient.OperationResult<BackupOperation> backup1 = icarusHolder.icarusClient.backup(backupOperationRequest1);
        IcarusClient.OperationResult<BackupOperation> backup2 = icarusHolder.icarusClient.backup(backupOperationRequest2);

        icarusHolder.icarusClient.waitForCompleted(backup1);
        icarusHolder.icarusClient.waitForCompleted(backup2);

        icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

        RestoreOperationRequest restoreRequest = createRestoreOperationRequest(icarusHolder.icarusClient.getCassandraSchemaVersion().getSchemaVersion());

        icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreRequest));

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
    protected final List<Path> dataDirs = Arrays.asList(cassandraDir.resolve("data").resolve("data"));
    protected final Path downloadDir = new File("target/downloaded").toPath().toAbsolutePath();
    protected final Path backupDir = Paths.get(target("backup1"));

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

}
