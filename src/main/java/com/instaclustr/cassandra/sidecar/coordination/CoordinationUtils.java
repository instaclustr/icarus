package com.instaclustr.cassandra.sidecar.coordination;

import static java.lang.String.format;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraEndpointDC;
import com.instaclustr.cassandra.backup.impl.interaction.CassandraEndpoints;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
import com.instaclustr.operations.OperationCoordinator.OperationCoordinatorException;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import jmx.org.apache.cassandra.service.CassandraJMXService;

public class CoordinationUtils {

    public static Map<InetAddress, UUID> getEndpoints(final CassandraJMXService cassandraJMXService) throws OperationCoordinatorException {
        return CoordinationUtils.getEndpoints(cassandraJMXService, null);
    }

    public static Map<InetAddress, UUID> getEndpoints(final CassandraJMXService cassandraJMXService, final String datacenter) throws OperationCoordinatorException {
        try {
            return new CassandraEndpoints(cassandraJMXService, datacenter).act();
        } catch (final Exception ex) {
            throw new OperationCoordinatorException("Unable to get endpoints!", ex);
        }
    }

    public static Map<InetAddress, String> getEndpointsDCs(final CassandraJMXService cassandraJMXService, final Collection<InetAddress> endpoints) throws OperationCoordinatorException {
        try {
            return new CassandraEndpointDC(cassandraJMXService, endpoints).act();
        } catch (final Exception ex) {
            throw new OperationCoordinatorException("Unable to get DCs of endpoints!", ex);
        }
    }

    public static Map<InetAddress, SidecarClient> constructSidecars(final Map<InetAddress, UUID> endpoints,
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
                    .withHostId(entry.getValue())
                    .withDc(endpointDcs.get(entry.getKey()))
                    .withObjectMapper(objectMapper)
                    .build();

            inetAddressSidecarMap.put(entry.getKey(), sidecar);
        }

        return inetAddressSidecarMap;
    }
}
