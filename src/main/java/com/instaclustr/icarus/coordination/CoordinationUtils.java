package com.instaclustr.icarus.coordination;

import static java.lang.String.format;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.instaclustr.esop.topology.CassandraClusterTopology.ClusterTopology;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.sidecar.picocli.SidecarSpec;

public class CoordinationUtils {

    public static Map<InetAddress, IcarusClient> constructSidecars(final ClusterTopology topology,
                                                                   final SidecarSpec icarusSpec,
                                                                   final ObjectMapper objectMapper) {

        final String clusterName = topology.clusterName;
        final Map<InetAddress, UUID> endpoints = topology.endpoints;
        final Map<InetAddress, String> endpointDcs = topology.endpointDcs;

        final Set<InetAddress> difference = Sets.difference(endpoints.keySet(), endpointDcs.keySet());

        if (!difference.isEmpty()) {
            throw new IllegalStateException(format("Map of endpoints to their host ids is not equal on keys with a map of endpoints to their DC: %s, second map: %s",
                                                   endpoints.toString(),
                                                   endpointDcs.toString()));
        }

        final Map<InetAddress, IcarusClient> inetAddressSidecarMap = new HashMap<>();

        for (final Map.Entry<InetAddress, UUID> entry : endpoints.entrySet()) {
            final IcarusClient icarusClient = new IcarusClient.Builder()
                    .withInetAddress(entry.getKey())
                    // here we assume that all sidecars would be on same port as this one
                    .withPort(icarusSpec.httpServerAddress.getPort())
                    .withClusterName(clusterName)
                    .withDc(endpointDcs.get(entry.getKey()))
                    .withHostId(entry.getValue())
                    .withObjectMapper(objectMapper)
                    .build();

            inetAddressSidecarMap.put(entry.getKey(), icarusClient);
        }

        return inetAddressSidecarMap;
    }
}
