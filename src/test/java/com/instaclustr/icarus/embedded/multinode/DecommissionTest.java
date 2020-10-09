package com.instaclustr.icarus.embedded.multinode;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.service.CassandraStatusService.Status.NodeState;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest;
import org.awaitility.Duration;
import org.testng.annotations.Test;

public class DecommissionTest extends AbstractCassandraIcarusTest {

    @Override
    protected Map<String, SidecarHolder> customSidecars() throws Exception {
        return twoSidecars();
    }

    @Override
    protected Map<String, Cassandra> customNodes() throws Exception {
        return twoNodes();
    }

    @Test
    public void decommissionTest() {

        IcarusClient firstClient = sidecars.get("datacenter1").icarusClient;
        IcarusClient secondClient = sidecars.get("datacenter2").icarusClient;

        secondClient.waitForCompleted(secondClient.decommission(true), Duration.TWO_MINUTES);

        assertEquals(secondClient.getStatus().status.getNodeState(), NodeState.DECOMMISSIONED);
        assertEquals(firstClient.getStatus().status.getNodeState(), NodeState.NORMAL);
    }
}
