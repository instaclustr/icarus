package com.instaclustr.sidecar.embedded.singlenode;

import static com.instaclustr.cassandra.backup.impl.restore.RestorationStrategy.RestorationStrategyType.IN_PLACE;
import static com.instaclustr.io.FileUtils.createDirectory;
import static com.instaclustr.io.FileUtils.deleteDirectory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import com.instaclustr.cassandra.backup.impl.StorageLocation;
import com.instaclustr.cassandra.backup.impl.backup.BackupOperationRequest;
import com.instaclustr.cassandra.backup.impl.restore.RestoreOperationRequest;
import com.instaclustr.cassandra.topology.CassandraClusterTopology;
import com.instaclustr.cassandra.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.measure.DataRate;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import com.instaclustr.sidecar.embedded.DatabaseHelper;

public abstract class AbstractSingleNodeBackupFromScratchRestoreTest extends AbstractCassandraSidecarTest {

    public static String BUCKET = UUID.randomUUID().toString();

    protected void backupTest(String cloud) throws Exception {

        AbstractCassandraSidecarTest.SidecarHolder sidecarHolder = sidecars.get("datacenter1");

        CassandraClusterTopology.ClusterTopology topology = sidecarHolder.sidecarClient.getCassandraClusterTopology("datacenter1");

        BackupOperationRequest backupOperationRequest1 = createBackupRequest(cloud, "snapshot1", topology);

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.backup(backupOperationRequest1));

        stopNodes();

        // remove data
        deleteDirectory(cassandraDir);
        createDirectory(cassandraDir.resolve("data"));

        RestoreOperationRequest restoreOperationRequest = createRestoreOperationRequest(cloud, "snapshot1", topology);

        sidecarHolder.sidecarClient.waitForCompleted(sidecarHolder.sidecarClient.restore(restoreOperationRequest));

        startNodes();

        DatabaseHelper helper = new DatabaseHelper(cassandraInstances, sidecars);

        helper.dump(keyspaceName, tableName);
    }

    private BackupOperationRequest createBackupRequest(final String cloud,
                                                       final String snapshotName,
                                                       final ClusterTopology topology) {

        final String clusterName = topology.clusterName;
        final String datacenter = topology.getDcs().stream().findFirst().get();
        final String nodeId = topology.topology.get(0).nodeId.toString();

        StorageLocation location;

        if (cloud.equals("s3") || cloud.equals("azure") || cloud.equals("gcp")) {
            location = new StorageLocation(String.format(cloud + "://" + BUCKET + "/%s/%s/%s", clusterName, datacenter, nodeId));
        } else {
            location = new StorageLocation(String.format("file://%s/%s/%s/%s", target("backup1"), clusterName, datacenter, nodeId));
        }

        return new BackupOperationRequest(
                "backup", // type
                location,
                null, // duration
                new DataRate(1L, DataRate.DataRateUnit.MBPS), // bandwidth
                15, // concurrentConnections
                null, // metadata
                cassandraDir.resolve("data"), // cassandra dir
                null, //DatabaseEntities.parse("system_schema," + keyspaceName), // entities
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

    private RestoreOperationRequest createRestoreOperationRequest(final String cloud,
                                                                  final String snapshotName,
                                                                  final ClusterTopology topology) {
        final String clusterName = topology.clusterName;
        final String datacenter = topology.getDcs().stream().findFirst().get();
        final String nodeId = topology.topology.get(0).nodeId.toString();
        final String schemaVersion = topology.schemaVersion;

        StorageLocation location;

        if (cloud.equals("s3") || cloud.equals("azure") || cloud.equals("gcp")) {
            location = new StorageLocation(String.format(cloud + "://" + BUCKET + "/%s/%s/%s", clusterName, datacenter, nodeId));
        } else {
            location = new StorageLocation(String.format("file://%s/%s/%s/%s", target("backup1"), clusterName, datacenter, nodeId));
        }

        return new RestoreOperationRequest(
                "restore", // type
                location,
                null, // concurrent connections
                null, // lock file
                cassandraDir.resolve("data"), // cassandra dir
                cassandraDir.resolve("config"), // cassandra config dir
                true, // restore system keyspace
                snapshotName + "-" + schemaVersion, // snapshot + schema version, it does not need to be there, just for testing purposes
                null, //DatabaseEntities.parse("system_schema," + keyspaceName), // entities
                false, // update cassandra yaml
                IN_PLACE, // restoration strategy
                null, // restoration phase
                null, // import
                false, // noDeleteTruncates
                false, // noDeleteDownload
                false, // noDownloadData
                false, // exactSchemaVersion
                null, // schema version
                null, // k8s namespace
                null, // k8s secret name
                false, // !!! GLOBAL REQUEST !!!
                null, // timeout,
                true, // resolve topology
                false // insecure
        );
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