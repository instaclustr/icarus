package com.instaclustr.icarus.embedded;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.WorkingDirectoryCustomizer;
import com.github.nosan.embedded.cassandra.commons.ClassPathResource;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.instaclustr.esop.impl.hash.HashSpec;
import com.instaclustr.icarus.Icarus;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.io.FileUtils;
import com.instaclustr.measure.Time;
import com.instaclustr.picocli.CassandraJMXSpec;
import com.instaclustr.picocli.typeconverter.CassandraJMXServiceURLTypeConverter;
import com.instaclustr.sidecar.picocli.SidecarSpec;
import com.instaclustr.sidecar.picocli.SidecarSpec.HttpServerInetSocketAddressTypeConverter;
import org.awaitility.Duration;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractCassandraIcarusTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractCassandraIcarusTest.class);

    protected static final String CASSANDRA_VERSION = System.getProperty("cassandra.version", "4.0-rc2");

    private final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();

    protected static final String keyspaceName = "testkeyspace";
    protected static final String tableName = "testtable";
    protected static final int numberOfSSTables = 3;

    protected Map<String, Cassandra> cassandraInstances = new TreeMap<>();
    protected Map<String, IcarusHolder> icaruses = new TreeMap<>();

    protected IcarusClient icarusClient;
    protected DatabaseHelper dbHelper;

    @BeforeClass
    public void beforeClass() throws Exception {
        startNodes();
        startSidecars();

        dbHelper = new DatabaseHelper(cassandraInstances, icaruses);
        icarusClient = icaruses.get("datacenter1").icarusClient;
    }

    @AfterClass
    public void afterClass() throws Exception {
        stopSidecars();
        stopNodes();
        waitForClosedPort(7199);
        waitForClosedPort("127.0.0.1", 7200);
        waitForClosedPort("127.0.0.2", 7200);
        customAfterClass();
    }

    public void customAfterClass() throws Exception {

    }

    @BeforeMethod
    public void beforeMethod() {
        dbHelper.createKeyspaceAndTable(keyspaceName, tableName);

        for (int i = 0; i < numberOfSSTables; i++) {
            dbHelper.addDataAndFlush(keyspaceName, tableName);
            dbHelper.addDataAndFlush(keyspaceName, tableName);
            dbHelper.addDataAndFlush(keyspaceName, tableName);
        }
    }

    @AfterMethod
    public void afterMethod() {
        try {
            dbHelper.dropKeyspace(keyspaceName);
        } catch (Exception ex) {
            // intentionally empty
        }
    }

    protected Cassandra getCassandra() throws Exception {
        CassandraBuilder builder = new CassandraBuilder();
        builder.workingDirectory(() -> cassandraDir)
               .version(CASSANDRA_VERSION)
               .jvmOptions("-Xmx1g", "-Xms1g", "-Dcassandra.ring_delay_ms=1000");

        if (CASSANDRA_VERSION.startsWith("2")) {
            FileUtils.createDirectory(cassandraDir.resolve("data").resolve("data"));

            builder.addConfigProperties(new HashMap<String, String[]>() {{
                put("data_file_directories", new String[]{
                        cassandraDir.resolve("data").resolve("data").toAbsolutePath().toString()
                });
            }});
        }

        return builder.build();
    }

    protected Map<String, Cassandra> customNodes() throws Exception {
        return Collections.emptyMap();
    }

    protected void startNodes() {

        try {
            Map<String, Cassandra> customNodes = customNodes();

            cassandraInstances.clear();

            if (customNodes.isEmpty()) {
                cassandraInstances.put("datacenter1", getCassandra());
            } else {
                cassandraInstances.putAll(customNodes);
            }

            for (final Cassandra c : cassandraInstances.values()) {
                c.start();
                String hostAddress = c.getSettings().getAddress().getHostAddress();
                waitForOpenPort(hostAddress, 9042);
            }
            logger.info("started");
        } catch (Exception ex) {
            logger.error("Some nodes could not be started. Be sure you have 127.0.0.2 interface too for tests which are starting with multiple Cassandra instances.");
        }
    }

    protected void stopNodes() {
        List<Cassandra> nodes = new ArrayList<>(cassandraInstances.values());
        Collections.reverse(nodes);
        nodes.forEach(Cassandra::stop);
    }

    protected void startSidecars() throws Exception {

        Map<String, IcarusHolder> customSidecars = customSidecars();

        icaruses.clear();

        if (customSidecars.isEmpty()) {
            icaruses.put("datacenter1", defaultIcarus());
        } else {
            icaruses.putAll(customSidecars);
        }
    }

    protected void stopSidecars() {
        List<IcarusHolder> serverServices = new ArrayList<>(icaruses.values());

        Collections.reverse(serverServices);
        serverServices.forEach(pair -> {
            try {
                pair.serviceManager.stopAsync().awaitStopped();
                pair.icarusClient.close();
                waitForClosedPort(pair.icarusClient.getHost(), pair.icarusClient.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    protected Map<String, IcarusHolder> customSidecars() throws Exception {
        return Collections.emptyMap();
    }

    protected IcarusHolder sidecar(String jmxAddress, Integer jmxPort, String httpAdddress, Integer httpPort, String datacenter) throws Exception {
        CassandraJMXSpec cassandraJMXSpec = new CassandraJMXSpec();
        cassandraJMXSpec.jmxServiceURL = new CassandraJMXServiceURLTypeConverter().convert("service:jmx:rmi:///jndi/rmi://" + jmxAddress + ":" + jmxPort.toString() + "/jmxrmi");

        SidecarSpec icarusSpec = new SidecarSpec();
        icarusSpec.httpServerAddress = new HttpServerInetSocketAddressTypeConverter().convert(httpAdddress + ":" + httpPort.toString());
        icarusSpec.operationsExpirationPeriod = new Time(1L, HOURS);

        HashSpec hashSpec = new HashSpec();

        List<AbstractModule> modules = new Icarus().getModules(icarusSpec, cassandraJMXSpec, hashSpec, true);

        Injector injector = Guice.createInjector(modules);

        ServiceManager serviceManager = injector.getInstance(ServiceManager.class);

        serviceManager.startAsync().awaitHealthy();

        waitForOpenPort(httpPort);

        IcarusClient icarusClient = new IcarusClient.Builder()
                .withHostAddress(httpAdddress)
                .withPort(httpPort)
                .withObjectMapper(injector.getInstance(ObjectMapper.class))
                .withDc(datacenter)
                .build(injector.getInstance(ResourceConfig.class));

        return new IcarusHolder(serviceManager, icarusClient, injector);
    }

    protected IcarusHolder defaultIcarus() throws Exception {
        return sidecar("127.0.0.1", 7199, "127.0.0.1", 4567, "datacenter1");
    }

    protected void waitForClosedPort(String hostname, int port) {
        await().timeout(Duration.FIVE_MINUTES).until(() -> {
            try {
                (new Socket(hostname, port)).close();
                return false;
            } catch (SocketException e) {
                return true;
            }
        });
    }

    protected void waitForClosedPort(int port) {
        waitForClosedPort("127.0.0.1", port);
    }

    protected void waitForOpenPort(String hostname, int port) {
        await().until(() -> {
            try {
                (new Socket("127.0.0.1", port)).close();
                return true;
            } catch (SocketException e) {
                return false;
            }
        });
    }

    protected void waitForOpenPort(int port) {
        waitForOpenPort("127.0.0.1", port);
    }

    public static class IcarusHolder {

        public final ServiceManager serviceManager;
        public final IcarusClient icarusClient;
        public final Injector injector;

        public IcarusHolder(final ServiceManager serviceManager,
                            final IcarusClient icarusClient,
                            final Injector injector) {
            this.serviceManager = serviceManager;
            this.icarusClient = icarusClient;
            this.injector = injector;
        }
    }

    public Map<String, IcarusHolder> twoSidecars() throws Exception {
        return new TreeMap<String, IcarusHolder>() {{
            put("datacenter1", sidecar("127.0.0.1", 7199, "127.0.0.1", 4567, "datacenter1"));
            put("datacenter2", sidecar("127.0.0.1", 7200, "127.0.0.2", 4567, "datacenter2"));
        }};
    }

    public TreeMap<String, Cassandra> twoNodes() throws Exception {
        CassandraBuilder builder = new CassandraBuilder();
        Path workDir = Files.createTempDirectory(null);
        builder.version(CASSANDRA_VERSION);
        builder.workingDirectory(() -> workDir);

        List<WorkingDirectoryCustomizer> customizers = new ArrayList<>();
        customizers.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("cassandra1-rackdc.properties"),
                                                               "conf/cassandra-rackdc.properties"));

        if (CASSANDRA_VERSION.startsWith("4")) {
            customizers.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("first-4.yaml"), "conf/cassandra.yaml"));
        } else if (CASSANDRA_VERSION.startsWith("2")) {
            customizers.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("first-2.yaml"), "conf/cassandra.yaml"));
        } else {
            customizers.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("first.yaml"), "conf/cassandra.yaml"));
        }

        builder.addSystemProperties(new HashMap<String, String>() {{
            put("cassandra.jmx.local.port", "7199");
            put("cassandra.ring_delay_ms", "1000");
        }});
        builder.addJvmOptions("-Xms1g", "-Xmx1g");

        if (CASSANDRA_VERSION.startsWith("2")) {
            FileUtils.createDirectory(workDir.resolve("data").resolve("data"));

            builder.addConfigProperties(new HashMap<String, String[]>() {{
                put("data_file_directories", new String[]{workDir.resolve("data").resolve("data").toAbsolutePath().toString()});
            }});
        }

        builder.addWorkingDirectoryCustomizers(customizers);
        Cassandra firstNode = builder.build();

        // SECOND NODE

        CassandraBuilder builder2 = new CassandraBuilder();
        builder2.version(CASSANDRA_VERSION);
        Path workDir2 = Files.createTempDirectory(null);
        builder2.workingDirectory(() -> workDir2);

        List<WorkingDirectoryCustomizer> customizers2 = new ArrayList<>();
        customizers2.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("cassandra2-rackdc.properties"),
                                                                "conf/cassandra-rackdc.properties"));

        if (CASSANDRA_VERSION.startsWith("4")) {
            customizers2.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("second-4.yaml"), "conf/cassandra.yaml"));
        } else if (CASSANDRA_VERSION.startsWith("2")) {
            customizers2.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("second-2.yaml"), "conf/cassandra.yaml"));
        } else {
            customizers2.add(WorkingDirectoryCustomizer.addResource(new ClassPathResource("second.yaml"), "conf/cassandra.yaml"));
        }


        builder2.addSystemProperties(new HashMap<String, String>() {{
            put("cassandra.jmx.local.port", "7200");
            put("cassandra.ring_delay_ms", "1000");
        }});

        builder2.addJvmOptions("-Xms1g", "-Xmx1g");

        if (CASSANDRA_VERSION.startsWith("2")) {
            FileUtils.createDirectory(workDir2.resolve("data").resolve("data"));

            builder2.addConfigProperties(new HashMap<String, String[]>() {{
                put("data_file_directories", new String[]{workDir.resolve("data").resolve("data").toAbsolutePath().toString()});
            }});
        }

        builder2.addWorkingDirectoryCustomizers(customizers2);
        Cassandra secondNode = builder2.build();

        return new TreeMap<String, Cassandra>() {{
            put("datacenter1", firstNode);
            put("datacenter2", secondNode);
        }};
    }
}
