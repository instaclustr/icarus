package com.instaclustr.cassandra.sidecar.coordination;

import static java.lang.String.format;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
import com.instaclustr.sidecar.picocli.SidecarSpec;

public class CoordinationUtils {

    public static Map<InetAddress, SidecarClient> constructSidecars(final String clusterName,
                                                                    final Map<InetAddress, UUID> endpoints,
                                                                    final Map<InetAddress, String> endpointDcs,
                                                                    final SidecarSpec sidecarSpec,
                                                                    final ObjectMapper objectMapper) {

        final Set<InetAddress> difference = Sets.difference(endpointDcs.keySet(), endpointDcs.keySet());

        if (!difference.isEmpty()) {
            throw new IllegalStateException(format("Map of endpoints to their host ids is not equal on keys with a map of endpoints to their DC: %s, second map: %s",
                                                   endpoints.toString(),
                                                   endpointDcs.toString()));
        }

        final Map<InetAddress, SidecarClient> inetAddressSidecarMap = new HashMap<>();

        for (final Map.Entry<InetAddress, UUID> entry : endpoints.entrySet()) {
            final SidecarClient sidecar = new SidecarClient.Builder()
                    .withInetAddress(entry.getKey())
                    // here we assume that all sidecars would be on same port as this one
                    .withPort(sidecarSpec.httpServerAddress.getPort())
                    .withClusterName(clusterName)
                    .withDc(endpointDcs.get(entry.getKey()))
                    .withHostId(entry.getValue())
                    .withObjectMapper(objectMapper)
                    .build();

            inetAddressSidecarMap.put(entry.getKey(), sidecar);
        }

        return inetAddressSidecarMap;
    }
}
