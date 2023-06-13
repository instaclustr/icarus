package com.instaclustr.icarus.resource;

import javax.inject.Inject;

import com.instaclustr.icarus.service.CassandraService;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/topology")
@Produces(MediaType.APPLICATION_JSON)
public class ClusterTopologyResource {

    private final CassandraService cassandraService;

    @Inject
    @jakarta.inject.Inject
    public ClusterTopologyResource(final CassandraService cassandraService) {
        this.cassandraService = cassandraService;
    }

    @GET
    public Response getCassandraTopology() {
        try {
            return Response.ok(cassandraService.getClusterTopology(null)).build();
        } catch (final Exception ex) {
            return Response.serverError().entity(ex).build();
        }
    }

    @GET
    @Path("{dc}")
    public Response getCassandraTopology(final @PathParam("dc") String dc) {
        try {
            return Response.ok(cassandraService.getClusterTopology(dc)).build();
        } catch (final Exception ex) {
            return Response.serverError().entity(ex).build();
        }
    }
}
