package com.instaclustr.icarus.resource;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;

@Path("/config")
@Produces("application/yaml")
public class CassandraConfigResource {

    @GET
    public Response getCassandraConfiguration() {
        try {
            final Optional<java.nio.file.Path> cassandraYaml = Files.walk(Paths.get("/var/lib/cassandra")).filter(p -> {
                String name = p.toFile().getName();

                return name.startsWith("cassandra") && name.endsWith(".yaml");
            }).findFirst();

            if (cassandraYaml.isPresent()) {
                return Response.ok(new String(Files.readAllBytes(cassandraYaml.get())), "application/yaml").build();
            } else {
                return Response.status(Response.Status.NOT_FOUND).build();
            }
        } catch (final Exception ex) {
            return Response.serverError().build();
        }
    }
}
