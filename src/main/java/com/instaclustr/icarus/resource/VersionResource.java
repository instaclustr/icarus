package com.instaclustr.icarus.resource;

import javax.inject.Inject;
import javax.inject.Provider;

import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.icarus.service.CassandraService;
import com.instaclustr.icarus.service.CassandraService.CassandraSchemaVersion;
import com.instaclustr.version.Version;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/version")
@Produces(MediaType.APPLICATION_JSON)
public class VersionResource {

    private final Version version;
    private final Provider<CassandraVersion> cassandraVersion;
    private final CassandraService cassandraService;

    @Inject
    @jakarta.inject.Inject
    public VersionResource(final Version version,
                           final Provider<CassandraVersion> cassandraVersion,
                           final CassandraService cassandraService) {
        this.version = version;
        this.cassandraVersion = cassandraVersion;
        this.cassandraService = cassandraService;
    }

    @GET
    public Response getVersion() {
        return getSidecarVersion();
    }

    @GET
    @Path("sidecar")
    public Response getSidecarVersion() {
        return Response.ok(version).build();
    }

    @GET
    @Path("cassandra")
    public Response getCassandraVersion() {
        return Response.ok(cassandraVersion.get()).build();
    }

    @GET
    @Path("schema")
    public Response getCassandraSchemaVersion() {
        final CassandraSchemaVersion schemaVersion = cassandraService.getCassandraSchemaVersion();

        if (schemaVersion.getException() != null) {
            return Response.serverError().entity(schemaVersion.getException()).build();
        }

        return Response.ok(schemaVersion).build();
    }
}
