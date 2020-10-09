package com.instaclustr.icarus.embedded.singlenode;

import static com.instaclustr.icarus.service.CassandraStatusService.Status.NodeState.DRAINED;
import static com.instaclustr.operations.Operation.State.COMPLETED;
import static org.testng.Assert.assertEquals;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import org.testng.annotations.Test;

public class DrainTest extends AbstractCassandraIcarusTest {

    @Test(expectedExceptions = AllNodesFailedException.class)
    public void drainTest() {
        icarusClient.waitForState(icarusClient.drain(), COMPLETED);

        assertEquals(icarusClient.getStatus().status.getNodeState(), DRAINED);

        // this should fail because we can not do anyting cql-ish once a node is drained
        dbHelper.createTable(keyspaceName, tableName);
    }
}
