package com.instaclustr.sidecar.embedded;

import static com.instaclustr.operations.OperationBindings.installOperationBindings;
import static java.util.concurrent.TimeUnit.HOURS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.nosan.embedded.cassandra.EmbeddedCassandraFactory;
import com.github.nosan.embedded.cassandra.api.Cassandra;
import com.github.nosan.embedded.cassandra.api.Version;
import com.github.nosan.embedded.cassandra.artifact.Artifact;
import com.github.nosan.embedded.cassandra.commons.io.ClassPathResource;
import com.google.common.util.concurrent.ServiceManager;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperation;
import com.instaclustr.cassandra.backup.impl.truncate.TruncateOperationRequest;
import com.instaclustr.cassandra.sidecar.Sidecar;
import com.instaclustr.cassandra.sidecar.rest.SidecarClient;
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

public abstract class AbstractCassandraSidecarTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractCassandraSidecarTest.class);

    protected static final String CASSANDRA_VERSION = System.getProperty("cassandra.version", "3.11.6");

    private static Artifact CASSANDRA_ARTIFACT = Artifact.ofVersion(Version.of(CASSANDRA_VERSION));

    private final Path cassandraDir = new File("target/cassandra").toPath().toAbsolutePath();

    protected static final String keyspaceName = "testkeyspace";
    protected static final String tableName = "testtable";
    protected static final int numberOfSSTables = 3;

    protected Map<String, Cassandra> cassandraInstances = new TreeMap<>();
    protected Map<String, SidecarHolder> sidecars = new TreeMap<>();

    protected SidecarClient sidecarClient;
    protected DatabaseHelper dbHelper;

    @BeforeClass
    public void beforeClass() throws Exception {
        startNodes();
        startSidecars();

        dbHelper = new DatabaseHelper(cassandraInstances, sidecars);
        sidecarClient = sidecars.get("datacenter1").sidecarClient;
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

    protected EmbeddedCassandraFactory defaultNodeFactory() {
        EmbeddedCassandraFactory cassandraInstanceFactory = new EmbeddedCassandraFactory();
        cassandraInstanceFactory.setWorkingDirectory(cassandraDir);
        cassandraInstanceFactory.setArtifact(CASSANDRA_ARTIFACT);
        cassandraInstanceFactory.getJvmOptions().add("-Xmx1g");
        cassandraInstanceFactory.getJvmOptions().add("-Xms1g");
        cassandraInstanceFactory.setJmxLocalPort(7199);

        return cassandraInstanceFactory;
    }

    protected Map<String, Cassandra> customNodes() throws Exception {
        return Collections.emptyMap();
    }

    protected void startNodes() {

        try {
            Map<String, Cassandra> customNodes = customNodes();

            if (customNodes.isEmpty()) {
                cassandraInstances.put("datacenter1", defaultNodeFactory().create());
            } else {
                cassandraInstances.putAll(customNodes);
            }

            cassandraInstances.values().forEach(Cassandra::start);
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

        Map<String, SidecarHolder> customSidecars = customSidecars();

        sidecars.clear();

        if (customSidecars.isEmpty()) {
            sidecars.put("datacenter1", defaultSidecar());
        } else {
            sidecars.putAll(customSidecars);
        }
    }

    protected void stopSidecars() {
        List<SidecarHolder> serverServices = new ArrayList<>(sidecars.values());

        Collections.reverse(serverServices);
        serverServices.forEach(pair -> {
            try {
                pair.serviceManager.stopAsync().awaitStopped();
                pair.sidecarClient.close();
                waitForClosedPort(pair.sidecarClient.getHost(), pair.sidecarClient.getPort());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    protected Map<String, SidecarHolder> customSidecars() throws Exception {
        return Collections.emptyMap();
    }

    protected SidecarHolder sidecar(String jmxAddress, Integer jmxPort, String httpAdddress, Integer httpPort) throws Exception {
        CassandraJMXSpec cassandraJMXSpec = new CassandraJMXSpec();
        cassandraJMXSpec.jmxServiceURL = new CassandraJMXServiceURLTypeConverter().convert("service:jmx:rmi:///jndi/rmi://" + jmxAddress + ":" + jmxPort.toString() + "/jmxrmi");

        SidecarSpec sidecarSpec = new SidecarSpec();
        sidecarSpec.httpServerAddress = new HttpServerInetSocketAddressTypeConverter().convert(httpAdddress + ":" + httpPort.toString());
        sidecarSpec.operationsExpirationPeriod = new Time(1L, HOURS);

        List<AbstractModule> modules = new Sidecar().getModules(sidecarSpec, cassandraJMXSpec, true);

        Injector injector = Guice.createInjector(modules);

        ServiceManager serviceManager = injector.getInstance(ServiceManager.class);

        serviceManager.startAsync().awaitHealthy();

        waitForOpenPort(httpPort);

        SidecarClient sidecarClient = new SidecarClient.Builder()
                .withHostAddress(httpAdddress)
                .withPort(httpPort)
                .withObjectMapper(injector.getInstance(ObjectMapper.class))
                .build(injector.getInstance(ResourceConfig.class));

        return new SidecarHolder(serviceManager, sidecarClient, injector);
    }

    protected SidecarHolder defaultSidecar() throws Exception {
        return sidecar("127.0.0.1", 7199, "127.0.0.1", 4567);
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

    public static class SidecarHolder {

        public final ServiceManager serviceManager;
        public final SidecarClient sidecarClient;
        public final Injector injector;

        public SidecarHolder(final ServiceManager serviceManager,
                             final SidecarClient sidecarClient,
                             final Injector injector) {
            this.serviceManager = serviceManager;
            this.sidecarClient = sidecarClient;
            this.injector = injector;
        }
    }

    public Map<String, SidecarHolder> twoSidecars() throws Exception {
        return new TreeMap<String, SidecarHolder>() {{
            put("datacenter1", sidecar("127.0.0.1", 7199, "127.0.0.1", 4567));
            put("datacenter2", sidecar("127.0.0.1", 7200, "127.0.0.2", 4567));
        }};
    }

    public TreeMap<String, Cassandra> twoNodes() throws Exception {
        EmbeddedCassandraFactory factory = defaultNodeFactory();

        factory.setRackConfig(new ClassPathResource("cassandra1-rackdc.properties"));
        factory.setWorkingDirectory(Files.createTempDirectory(null));

        if (CASSANDRA_VERSION.startsWith("4")) {
            factory.setConfig(new ClassPathResource("first-4.yaml"));
        } else {
            factory.setConfig(new ClassPathResource("first.yaml"));
        }

        factory.setJmxLocalPort(7199);

        Cassandra firstNode = factory.create();

        factory.setRackConfig(new ClassPathResource("cassandra2-rackdc.properties"));
        factory.setWorkingDirectory(Files.createTempDirectory(null));

        if (CASSANDRA_VERSION.startsWith("4")) {
            factory.setConfig(new ClassPathResource("second-4.yaml"));
        } else {
            factory.setConfig(new ClassPathResource("second.yaml"));
        }

        factory.setJmxLocalPort(7200);

        Cassandra secondNode = factory.create();

        return new TreeMap<String, Cassandra>() {{
            put("datacenter1", firstNode);
            put("datacenter2", secondNode);
        }};
    }
}
