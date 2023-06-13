package com.instaclustr.icarus.resource;

import com.google.inject.Inject;
import com.instaclustr.icarus.service.CassandraStatusService;
import com.instaclustr.icarus.service.CassandraStatusService.Status;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/status")
@Produces(MediaType.APPLICATION_JSON)
public class StatusResource {

    private final CassandraStatusService statusService;

    @Inject
    @jakarta.inject.Inject
    public StatusResource(final CassandraStatusService statusService) {
        this.statusService = statusService;
    }

    @GET
    public Response getStatus() {

        final Status status = statusService.getStatus();

        if (status.getException() != null) {
            return Response.serverError().entity(status).build();
        }

        return Response.ok(status).build();
    }

}
