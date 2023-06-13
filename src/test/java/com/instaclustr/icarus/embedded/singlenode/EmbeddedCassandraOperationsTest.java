package com.instaclustr.icarus.embedded.singlenode;

import com.google.common.collect.ImmutableSet;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.operations.cleanup.CleanupOperationRequest;
import com.instaclustr.icarus.operations.flush.FlushOperationRequest;
import com.instaclustr.icarus.operations.refresh.RefreshOperationRequest;
import com.instaclustr.icarus.operations.scrub.ScrubOperationRequest;
import com.instaclustr.icarus.operations.upgradesstables.UpgradeSSTablesOperationRequest;
import com.instaclustr.icarus.service.CassandraService.CassandraSchemaVersion;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class EmbeddedCassandraOperationsTest extends AbstractCassandraIcarusTest {

    @Test
    public void flushTest() {
        icarusClient.waitForCompleted(icarusClient.flush(new FlushOperationRequest(keyspaceName, Collections.singleton(tableName))));
        icarusClient.waitForCompleted(icarusClient.flush(new FlushOperationRequest(keyspaceName, ImmutableSet.of())));
    }

    @Test
    public void refreshTest() {
        icarusClient.waitForCompleted(icarusClient.refresh(new RefreshOperationRequest(keyspaceName, tableName)));
    }

    @Test
    public void cleanupTest() {
        icarusClient.waitForCompleted(icarusClient.cleanup(new CleanupOperationRequest(keyspaceName, Collections.singleton(tableName), 0)));
    }

    @Test
    public void scrubTest() {
        icarusClient.waitForCompleted(icarusClient.scrub(new ScrubOperationRequest("scrub", false, false, false, false, 0, keyspaceName, ImmutableSet.of())));
    }

    @Test
    public void upgradeSSTables() {
        icarusClient.waitForCompleted(icarusClient.upgradeSSTables(new UpgradeSSTablesOperationRequest("upgradesstables",
                                                                                                       keyspaceName,
                                                                                                       Collections.singleton(tableName),
                                                                                                       true,
                                                                                                       0)));
    }

    @Test
    public void cassandraSchemaVersion() {
        final CassandraSchemaVersion cassandraSchemaVersion = icarusClient.getCassandraSchemaVersion();
        assertNotNull(cassandraSchemaVersion.getSchemaVersion());
    }

    @Test
    public void sidecarVersion() {
        final String sidecarVersion = icarusClient.getIcarusVersion();
        assertNotNull(sidecarVersion);
    }

    @Test
    @Ignore
    public void cassandraVersion() {
        final CassandraVersion cassandraVersion = icarusClient.getCassandraVersion();

        assertTrue(cassandraVersion.getMajor() >= 3);
        assertNotNull(cassandraVersion);
    }
}
