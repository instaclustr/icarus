package com.instaclustr.icarus.embedded;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.instaclustr.icarus.embedded.AbstractCassandraIcarusTest.IcarusHolder;
import com.instaclustr.icarus.operations.flush.FlushOperationRequest;
import com.instaclustr.icarus.rest.IcarusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.insertInto;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

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

    public IcarusClient getClient()
    {
        return currentClient;
    }

    public void switchHelper(String datacenter) {
        currentNode = nodes.getOrDefault(datacenter, nodes.get(defaultDC));
        currentClient = icarusClients.getOrDefault(datacenter, icarusClients.get(defaultDC)).icarusClient;
    }

    public void createKeyspace(String keyspace, Map<String, Integer> networkTopologyMap) {
        withSession(session -> session.execute(SchemaBuilder.createKeyspace(keyspace).ifNotExists().withNetworkTopologyStrategy(networkTopologyMap).build()));
    }

    public void createKeyspace(String keyspace) {
        createKeyspace(keyspace, new HashMap<String, Integer>() {{
            put(defaultDC, 1);
        }});
    }

    public void createKeyspaceAndTable(String keyspace, String table) {
        createKeyspace(keyspace);
        createTable(keyspace, table);
    }

    public void createTable(String keyspace, String table) {
        withSession(session -> session.execute(SchemaBuilder.createTable(keyspace, table)
                                                            .ifNotExists()
                                                            .withPartitionKey("id", TEXT)
                                                            .withColumn("name", TEXT)
                                                            .build()));
    }

    public void dropKeyspace(final String keyspaceName) {
        withSession(session -> session.execute(SchemaBuilder.dropKeyspace(keyspaceName).build()));
    }

    public void dropTable(final String tableName) {
        withSession(session -> session.execute(SchemaBuilder.dropTable(tableName).build()));
    }

    public void truncateTable(final String tableName) {
        withSession(session -> session.execute(QueryBuilder.truncate(tableName).build()));
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
        withSession(session -> session.execute(insertInto(keyspaceName, tableName)
                                                       .value("id", literal(primaryKey))
                                                       .value("name", literal("stefan1"))
                                                       .build()));
    }

    public void addDataAndFlush(String keyspaceName, String tableName) {
        addDataAndFlush(keyspaceName, tableName, UUID.randomUUID().toString());
    }

    public void addDataAndFlush(String keyspaceName, String tableName, String primaryKey) {
        addData(keyspaceName, tableName, primaryKey);
        currentClient.waitForCompleted(currentClient.flush(new FlushOperationRequest(keyspaceName, Collections.singleton(tableName))));
    }

    public void dump(String keyspace, String table) {
        withSession(session -> {
            List<Row> all = session.execute(selectFrom(keyspace, table).all().build()).all();
            Assert.assertFalse(all.isEmpty());
            all.forEach(row -> logger.info(String.format("id: %s, name: %s", row.getString("id"), row.getString("name"))));
        });
    }

    private CqlSession getSession() {
        return new CqlSessionBuilder().addContactPoint(new InetSocketAddress(currentNode.getSettings().getAddress(),
                                                                             currentNode.getSettings().getPort()))
                                      .withLocalDatacenter(currentClient.getDc()).build();
    }

    private void withSession(Consumer<CqlSession> sessionConsumer) {
        try (final CqlSession session = getSession()) {
            sessionConsumer.accept(session);
        }
    }
}
