package com.instaclustr.icarus.embedded;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.github.nosan.embedded.cassandra.api.connection.CqlSessionCassandraConnectionFactory;
import com.instaclustr.icarus.operations.flush.FlushOperationRequest;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest.IcarusHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class DatabaseHelper {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseHelper.class);

    private static final String defaultDC = "datacenter1";

    private final Map<String, Cassandra> nodes;
    private final Map<String, IcarusHolder> icarusClients;

    private Cassandra currentNode;
    private IcarusClient currentClient;

    public DatabaseHelper(Map<String, Cassandra> nodes, Map<String, IcarusHolder> icarusClients) {
        this.nodes = nodes;
        this.icarusClients = icarusClients;
        switchHelper(defaultDC);
    }

    public void switchHelper(String datacenter) {
        currentNode = nodes.getOrDefault(datacenter, nodes.get(defaultDC));
        currentClient = icarusClients.getOrDefault(datacenter, icarusClients.get(defaultDC)).icarusClient;
    }

    public void createKeyspace(String keyspace, Map<String, Integer> networkTopologyMap) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(SchemaBuilder.createKeyspace(keyspace).ifNotExists().withNetworkTopologyStrategy(networkTopologyMap).build());
        }
    }

    public void createKeyspace(String keyspace) {
        Map<String, Integer> topologyMap = new HashMap<String, Integer>() {{
            put(defaultDC, 1);
        }};

        createKeyspace(keyspace, topologyMap);
    }

    public void createKeyspaceAndTable(String keyspace, String table) {
        createKeyspace(keyspace);
        createTable(keyspace, table);
    }

    public void createTable(String keyspace, String table) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(SchemaBuilder.createTable(keyspace, table)
                                         .ifNotExists()
                                         .withPartitionKey("id", TEXT)
                                         .withColumn("name", TEXT)
                                         .build());
        }
    }

    public void dropKeyspace(final String keyspaceName) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(SchemaBuilder.dropKeyspace(keyspaceName).build());
        }
    }

    public void dropTable(final String tableName) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(SchemaBuilder.dropTable(tableName).build());
        }
    }

    public void truncateTable(final String tableName) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(QueryBuilder.truncate(tableName).build());
        }
    }

    public void addData(int records, String keyspace, String table) {
        for (int i = 0; i < records; i++) {
            addData(keyspace, table, UUID.randomUUID().toString());
        }
    }

    public void addData(String keyspace, String table) {
        addData(1, keyspace, table);
    }

    public void addData(String keyspaceName, String tableName, String primaryKey) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            session.execute(insertInto(keyspaceName, tableName)
                                    .value("id", literal(primaryKey))
                                    .value("name", literal("stefan1"))
                                    .build());
        }
    }

    public void addDataAndFlush(String keyspaceName, String tableName) {
        addDataAndFlush(keyspaceName, tableName, UUID.randomUUID().toString());
    }

    public void addDataAndFlush(String keyspaceName, String tableName, String primaryKey) {
        addData(keyspaceName, tableName, primaryKey);
        currentClient.waitForCompleted(currentClient.flush(new FlushOperationRequest(keyspaceName, Collections.singleton(tableName))));
    }

    public void dump(String keyspace, String table) {
        try (CqlSession session = new CqlSessionCassandraConnectionFactory().create(currentNode).getConnection()) {
            List<Row> all = session.execute(selectFrom(keyspace, table).all().build()).all();
            Assert.assertFalse(all.isEmpty());
            all.forEach(row -> logger.info(String.format("id: %s, name: %s", row.getString("id"), row.getString("name"))));
        }
    }
}
