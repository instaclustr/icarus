package com.instaclustr.sidecar.embedded.singlenode;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Collections;

import com.google.common.collect.ImmutableSet;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.sidecar.operations.cleanup.CleanupOperationRequest;
import com.instaclustr.cassandra.sidecar.operations.flush.FlushOperationRequest;
import com.instaclustr.cassandra.sidecar.operations.refresh.RefreshOperationRequest;
import com.instaclustr.cassandra.sidecar.operations.scrub.ScrubOperationRequest;
import com.instaclustr.cassandra.sidecar.operations.upgradesstables.UpgradeSSTablesOperationRequest;
import com.instaclustr.cassandra.sidecar.service.CassandraSchemaVersionService.CassandraSchemaVersion;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

public class EmbeddedCassandraOperationsTest extends AbstractCassandraSidecarTest {

    @Test
    public void flushTest() {
        sidecarClient.waitForCompleted(sidecarClient.flush(new FlushOperationRequest(keyspaceName, Collections.singleton(tableName))));
        sidecarClient.waitForCompleted(sidecarClient.flush(new FlushOperationRequest(keyspaceName, ImmutableSet.of())));
    }

    @Test
    public void refreshTest() {
        sidecarClient.waitForCompleted(sidecarClient.refresh(new RefreshOperationRequest(keyspaceName, tableName)));
    }

    @Test
    public void cleanupTest() {
        sidecarClient.waitForCompleted(sidecarClient.cleanup(new CleanupOperationRequest(keyspaceName, Collections.singleton(tableName), 0)));
    }

    @Test
    public void scrubTest() {
        sidecarClient.waitForCompleted(sidecarClient.scrub(new ScrubOperationRequest("scrub", false, false, false, false, 0, keyspaceName, ImmutableSet.of())));
    }

    @Test
    public void upgradeSSTables() {
        sidecarClient.waitForCompleted(sidecarClient.upgradeSSTables(new UpgradeSSTablesOperationRequest("upgradesstables",
                                                                                                         keyspaceName,
                                                                                                         Collections.singleton(tableName),
                                                                                                         true,
                                                                                                         0)));
    }

    @Test
    public void cassandraSchemaVersion() {
        final CassandraSchemaVersion cassandraSchemaVersion = sidecarClient.getCassandraSchemaVersion();
        assertNotNull(cassandraSchemaVersion.getSchemaVersion());
    }

    @Test
    public void sidecarVersion() {
        final String sidecarVersion = sidecarClient.getSidecarVersion();
        assertNotNull(sidecarVersion);
    }

    @Test
    @Ignore
    public void cassandraVersion() {
        final CassandraVersion cassandraVersion = sidecarClient.getCassandraVersion();

        assertTrue(cassandraVersion.getMajor() >= 3);
        assertNotNull(cassandraVersion);
    }
}
