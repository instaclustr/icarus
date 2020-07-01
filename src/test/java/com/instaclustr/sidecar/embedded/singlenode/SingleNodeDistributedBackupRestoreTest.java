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
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.measure.DataRate;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import org.testng.annotations.Test;

/**
 * This tests invariant - what happens when we are doing distributed backup / restore on a cluster consisting of just one node.
 * It means that it will send these phase requests to itself only.
 */
public class SingleNodeDistributedBackupRestoreTest extends AbstractCassandraSidecarTest {

    @Test
    public void backupTest() {

        final SidecarHolder sidecarHolder = sidecars.get("datacenter1");

        final BackupOperationRequest backupOperationRequest = new BackupOperationRequest(
                "backup", // type
                new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/global"), // storage
                null, // duration
                new DataRate(1L, DataRate.DataRateUnit.MBPS), // bandwidth
                null, // concurrentConnections
                null, // lockFile
                cassandraDir.resolve("data"), // cassandra dir
                DatabaseEntities.parse("system_schema," + keyspaceName), // entities
                "stefansnapshot", // snapshot
                "default", // k8s namespace
                "test-sidecar-secret", // k8s secret
                true, // !!! GLOBAL REQUEST !!!
                null, // DC is null so will backup all datacenters
                false // keep existing snapshot
        );

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.backup(backupOperationRequest));


        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

        final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest(
                "restore", // type
                new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/global"), // storage location
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
                false, // noDeleteTruncates
                false, // noDeleteDownload
                false, // noDownloadData
                false, // exactSchemaVersion
                null, // schema version
                null, // k8s namespace
                null, // k8s secret name
                true // !!! GLOBAL REQUEST !!!
        );

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));

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
