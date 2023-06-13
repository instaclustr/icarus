package com.instaclustr.icarus.embedded.singlenode;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.service.CassandraStatusService;
import org.testng.annotations.Test;

import static com.instaclustr.operations.Operation.State.COMPLETED;

public class DrainTest extends AbstractCassandraIcarusTest {

    @Test(expectedExceptions = AllNodesFailedException.class)
    public void drainTest() {
        icarusClient.waitForState(icarusClient.drain(), COMPLETED);

        IcarusClient.StatusResult status = icarusClient.getStatus();
        CassandraStatusService.Status status2 = status.status;
        CassandraStatusService.Status.NodeState nodeState = status2.getNodeState();

        //assertEquals(icarusClient.getStatus().status.getNodeState(), DRAINED);

        // this should fail because we can not do anything cql-ish once a node is drained
        dbHelper.createTable(keyspaceName, tableName);
    }
}
