package com.instaclustr.icarus.embedded.singlenode;

import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.list.ListOperation;
import com.instaclustr.esop.impl.list.ListOperationRequest;
import com.instaclustr.esop.impl.remove.RemoveBackupRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.retry.RetrySpec;
import com.instaclustr.esop.topology.CassandraClusterTopology;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.embedded.DatabaseHelper;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.measure.DataRate;
import com.instaclustr.operations.Operation;
import com.instaclustr.operations.OperationRequest;
import org.testng.Assert;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.IN_PLACE;
import static com.instaclustr.io.FileUtils.createDirectory;
import static com.instaclustr.io.FileUtils.deleteDirectory;
import static org.testng.Assert.*;

public abstract class AbstractSingleNodeBackupFromScratchRestoreTest extends AbstractCassandraIcarusTest {

    public static String BUCKET = UUID.randomUUID().toString();

    protected void backupTest(String cloud) throws Exception {

        IcarusHolder icarusHolder = icaruses.get("datacenter1");

        CassandraClusterTopology.ClusterTopology topology = icarusHolder.icarusClient.getCassandraClusterTopology("datacenter1");
        final StorageLocation location = getStorageLocation(cloud, topology);

        BackupOperationRequest backupOperationRequest1 = createBackupRequest(location,"snapshot1");
        icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.backup(backupOperationRequest1));

        stopNodes();

        // remove data
        deleteDirectory(cassandraDir);
        createDirectory(cassandraDir.resolve("data"));

        RestoreOperationRequest restoreOperationRequest = createRestoreOperationRequest(location, "snapshot1", topology);
        icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreOperationRequest));

        startNodes();

        // list backups
        Operation<ListOperationRequest> listOperation = listBackups(location, icarusHolder);
        assertFalse(listOperation.request.response.reports.isEmpty());
        int numberOfBackups = listOperation.request.response.reports.size();

        final String backupNameToRemove = listOperation.request.response.reports.get(0).name;

        // remove backups
        icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.remove(createRemoveBackupRequest(location, backupNameToRemove)));

        Operation<ListOperationRequest> listOperationAfterBackupRemoval = listBackups(location, icarusHolder);
        int newNumberOfBackups = listOperationAfterBackupRemoval.request.response.reports.size();

        // we deleted one backup
        Assert.assertEquals(numberOfBackups - 1, newNumberOfBackups);

        DatabaseHelper helper = new DatabaseHelper(cassandraInstances, icaruses);

        helper.dump(keyspaceName, tableName);
    }

    private Operation<ListOperationRequest> listBackups(StorageLocation location,
                                                        IcarusHolder icarusHolder) {
        ListOperationRequest listRequest = createListRequest(location);
        IcarusClient.OperationResult<ListOperation> listResult = icarusHolder.icarusClient.list(listRequest);
        icarusHolder.icarusClient.waitForCompleted(listResult);
        Operation<ListOperationRequest> operation = icarusHolder.icarusClient.getOperation(listResult.operation.id, ListOperationRequest.class);
        return operation;
    }

    private RemoveBackupRequest createRemoveBackupRequest(final StorageLocation storageLocation,
                                                          final String backupName) {
        return new RemoveBackupRequest("remove-backup",
                                       storageLocation,
                                       null,
                                       null,
                                       false,
                                       false,
                                       null,
                                       null,
                                       backupName,
                                       false,
                                       true,
                                       null,
                                       esopCacheDir,
                                       false,
                                       null);
    }

    private ListOperationRequest createListRequest(final StorageLocation storageLocation) {
        return new ListOperationRequest("list",
                                        storageLocation,
                                        null,
                                        null,
                                        false,
                                        false,
                                        null,
                                        null,
                                        false,
                                        true,
                                        false,
                                        null,
                                        false,
                                        null,
                                        null,
                                        false,
                                        esopCacheDir,
                                        true,
                                        null,
                                        null);
    }

    private StorageLocation getStorageLocation(final String cloud,
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

        return location;
    }

    private BackupOperationRequest createBackupRequest(final StorageLocation location, final String snapshotName) {
        return new BackupOperationRequest(
                "backup", // type
                location,
                null, // duration
                new DataRate(1L, DataRate.DataRateUnit.MBPS), // bandwidth
                15, // concurrentConnections
                null, // metadata
                null, //DatabaseEntities.parse("system_schema," + keyspaceName), // entities
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
                null, // proxy settings
                new RetrySpec(10, RetrySpec.RetryStrategy.EXPONENTIAL, 3, true), // retry
                false, // skip refreshing
                dataDirs
        );
    }

    private RestoreOperationRequest createRestoreOperationRequest(final StorageLocation location,
                                                                  final String snapshotName,
                                                                  final ClusterTopology topology) {
        final String schemaVersion = topology.schemaVersion;
        return new RestoreOperationRequest(
                "restore", // type
                location,
                null, // concurrent connections
                cassandraDir.resolve("data"), // cassandra dir
                cassandraDir.resolve("config"), // cassandra config dir
                true, // restore system keyspace
                false, // restore system auth keyspace
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
                null, // dc
                null, // timeout,
                true, // resolve topology
                false, // insecure
                false, // new cluster
                false, // skip bucket verification
                null, // proxy settings
                null, // rename
                new RetrySpec(10, RetrySpec.RetryStrategy.EXPONENTIAL, 3, true), // retry
                false,
                dataDirs
        );
    }

    @Override
    public void customAfterClass() throws Exception {
        deleteDirectory(cassandraDir);
        deleteDirectory(downloadDir);
        deleteDirectory(backupDir);
        deleteDirectory(esopCacheDir);
    }

    protected final Path target = new File("target").toPath().toAbsolutePath();
    protected final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();
    protected final Path downloadDir = new File("target/downloaded").toPath().toAbsolutePath();
    protected final Path backupDir = Paths.get(target("backup1"));

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }
}
