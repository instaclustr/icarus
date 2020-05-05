package com.instaclustr.sidecar.embedded.multinode;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
import com.instaclustr.cassandra.sidecar.service.CassandraStatusService.Status.NodeState;
import com.instaclustr.sidecar.embedded.AbstractCassandraSidecarTest;
import org.awaitility.Duration;
import org.testng.annotations.Test;

public class DecommissionTest extends AbstractCassandraSidecarTest {

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

        SidecarClient firstClient = sidecars.get("datacenter1").sidecarClient;
        SidecarClient secondClient = sidecars.get("datacenter2").sidecarClient;

        secondClient.waitForCompleted(secondClient.decommission(true), Duration.TWO_MINUTES);

        assertEquals(secondClient.getStatus().status.getNodeState(), NodeState.DECOMMISSIONED);
        assertEquals(firstClient.getStatus().status.getNodeState(), NodeState.NORMAL);
    }
}
