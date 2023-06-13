package com.instaclustr.icarus.embedded.multinode;

import com.github.nosan.embedded.cassandra.Cassandra;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.service.CassandraStatusService.Status.NodeState;
import org.awaitility.Duration;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;

public class DecommissionTest extends AbstractCassandraIcarusTest {

    @Override
    protected Map<String, IcarusHolder> customSidecars() throws Exception {
        return twoSidecars();
    }

    @Override
    protected Map<String, Cassandra> customNodes() throws Exception {
        return twoNodes();
    }

    @Test
    public void decommissionTest() {

        IcarusClient firstClient = icaruses.get("datacenter1").icarusClient;
        IcarusClient secondClient = icaruses.get("datacenter2").icarusClient;

        secondClient.waitForCompleted(secondClient.decommission(true), Duration.TWO_MINUTES);

        assertEquals(secondClient.getStatus().status.getNodeState(), NodeState.DECOMMISSIONED);
        assertEquals(firstClient.getStatus().status.getNodeState(), NodeState.NORMAL);
    }
}
