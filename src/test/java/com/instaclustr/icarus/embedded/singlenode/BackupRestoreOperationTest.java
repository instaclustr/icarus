package com.instaclustr.icarus.embedded.singlenode;

import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.CLEANUP;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.DOWNLOAD;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.IMPORT;
import static com.instaclustr.esop.impl.restore.RestorationPhase.RestorationPhaseType.TRUNCATE;
import static com.instaclustr.esop.impl.restore.RestorationStrategy.RestorationStrategyType.HARDLINKS;

import com.instaclustr.esop.impl.DatabaseEntities;
import com.instaclustr.esop.impl.StorageLocation;
import com.instaclustr.esop.impl._import.ImportOperationRequest;
import com.instaclustr.esop.impl.backup.BackupOperation;
import com.instaclustr.esop.impl.backup.BackupOperationRequest;
import com.instaclustr.esop.impl.restore.RestoreOperationRequest;
import com.instaclustr.esop.impl.truncate.TruncateOperationRequest;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.rest.IcarusClient.OperationResult;
import com.instaclustr.io.FileUtils;
import org.testng.annotations.Test;

public class BackupRestoreOperationTest extends AbstractCassandraIcarusTest {

    @Test
    public void backupTest() throws Exception {

        try {
            final IcarusHolder icarusHolder = icaruses.get("datacenter1");

            final BackupOperationRequest backupOperationRequest = new BackupOperationRequest(
                    "backup",
                    new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1"),
                    null, // duration
                    null, // bandwidth
                    null, // concurrent connections
                    null, // metadata directive
                    DatabaseEntities.parse(keyspaceName),
                    "stefansnapshot", // snapshot
                    "default", // k8s namespace
                    "test-sidecar-secret", // k8s secret
                    false, // global request
                    null, // dc
                    null, // timeout
                    false,  // insecure
                    false, // create missing bucket
                    false, // skip bucket verification
                    null, // schemaVersion,
                    false, // upload topology
                    null, // proxy
                    null, // retry
                    false, // skip refreshing
                    dataDirs
            );

            final OperationResult<BackupOperation> result = icarusHolder.icarusClient.backup(backupOperationRequest);
            icarusHolder.icarusClient.waitForCompleted(result);

            // wait 10 seconds and do another backup, with exact same request, manifest will differ just on timestamp

            // put more data

            logger.info(String.format("content of %s.%s before adding more data", keyspaceName, tableName));

            dbHelper.dump(keyspaceName, tableName);

            dbHelper.addData(keyspaceName, tableName);

            logger.info(String.format("content of %s.%s after adding more data", keyspaceName, tableName));

            dbHelper.dump(keyspaceName, tableName);

            Thread.sleep(10000);

            final OperationResult<BackupOperation> result2 = icarusHolder.icarusClient.backup(backupOperationRequest);
            icarusHolder.icarusClient.waitForCompleted(result2);

            icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.truncate(new TruncateOperationRequest(keyspaceName, tableName)));

            // restore, it will restore into the latest one

            final RestoreOperationRequest restoreOperationRequest = new RestoreOperationRequest(
                    "restore", // type
                    new StorageLocation("file://" + target("backup1") + "/cluster/test-dc/1"), // storage location
                    null, // concurrent connections
                    cassandraDir.resolve("data"), // cassandra dir
                    cassandraDir.resolve("config"), // cassandra config dir
                    false, // restore system keyspace
                    false, // restore system auth keyspace
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
                    null, // dc
                    null, // timeout
                    false, // resolveHostIdFromTopology
                    false, // insecure
                    false, // newCluster
                    false, // skipBucketVerification
                    null, // proxy
                    null, // rename
                    null, // retry
                    false, // single
                    dataDirs
            );

            icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreOperationRequest));

            restoreOperationRequest.restorationPhase = TRUNCATE;
            icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreOperationRequest));

            restoreOperationRequest.restorationPhase = IMPORT;
            icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreOperationRequest));
            restoreOperationRequest.restorationPhase = CLEANUP;

            icarusHolder.icarusClient.waitForCompleted(icarusHolder.icarusClient.restore(restoreOperationRequest));

            logger.info(String.format("content of %s.%s after restore", keyspaceName, tableName));
            dbHelper.dump(keyspaceName, tableName);
        } catch (final Exception ex) {
            FileUtils.deleteDirectory(downloadDir);
        }
    }

    protected String target(final String path) {
        return target.resolve(path).toAbsolutePath().toString();
    }

}
