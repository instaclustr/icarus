package com.instaclustr.operations;


import static com.instaclustr.operations.OperationBindings.installOperationBindings;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.validation.Validation;
import javax.validation.Validator;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;
import com.instaclustr.cassandra.CassandraVersion;
import com.instaclustr.cassandra.service.CassandraWaiter;
import com.instaclustr.cassandra.service.CqlSessionService;
import com.instaclustr.icarus.Icarus;
import com.instaclustr.icarus.rest.IcarusClient;
import com.instaclustr.icarus.rest.IcarusClient.OperationResult;
import com.instaclustr.jackson.JacksonModule;
import com.instaclustr.sidecar.http.JerseyHttpServerModule;
import com.instaclustr.sidecar.http.JerseyHttpServerService;
import com.instaclustr.threading.ExecutorsModule;
import jmx.org.apache.cassandra.service.CassandraJMXService;
import org.apache.commons.lang3.tuple.Pair;
import org.glassfish.jersey.server.ResourceConfig;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractIcarusTest {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractIcarusTest.class);

    @Inject
    ResourceConfig resourceConfig;

    @Inject
    ObjectMapper objectMapper;

    JerseyHttpServerService serverService;

    IcarusClient icarusClient;

    Validator validator;

    protected List<Module> getModules() {
        return ImmutableList.of();
    }

    @BeforeMethod
    public void setup() {

        List<Module> modules = new ArrayList<Module>() {{
            add(new OperationsModule(3600));
            add(new AbstractModule() {
                @Override
                protected void configure() {

                    final CassandraJMXService mock = mock(CassandraJMXService.class);
                    final CqlSessionService cqlSessionServiceMock = mock(CqlSessionService.class);
                    final CassandraWaiter cassandraWaiterMock = mock(CassandraWaiter.class);
                    final CassandraVersion cassandraVersionMock = mock(CassandraVersion.class);

                    try {

                        when(mock.doWithStorageServiceMBean(any())).then(new Answer<Object>() {
                            @Override
                            public Object answer(final InvocationOnMock invocation) {
                                return 0;
                            }
                        });

                        when(cassandraVersionMock.getMajor()).thenReturn(3);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    bind(CassandraJMXService.class).toInstance(mock);
                    bind(CqlSessionService.class).toInstance(cqlSessionServiceMock);
                    bind(CassandraWaiter.class).toInstance(cassandraWaiterMock);
                    bind(CassandraVersion.class).toProvider(new Provider<CassandraVersion>() {
                        public CassandraVersion get() {
                            return cassandraVersionMock;
                        }
                    });
                }
            });
            add(new JerseyHttpServerModule());
            add(new ExecutorsModule());
            add(new JacksonModule());
            addAll(Icarus.operationModules());
            add(new AbstractModule() {
                @Override
                protected void configure() {
                    installOperationBindings(binder(),
                                             "failing",
                                             FailingOperationRequest.class,
                                             FailingOperation.class);
                }
            });
        }};

        modules.addAll(getModules());

        final Injector injector = Guice.createInjector(modules);

        injector.injectMembers(this);

        // Port 0 will be a randomly assigned ephemeral port by the OS - each concrete class will get it's own port
        // Note we won't know the address till a socket actually binds on it, so we can't use this again, which is why
        // we call getServerInetAddress from the serverService rather than using the passed in InetSocketAddress

        serverService = new JerseyHttpServerService(new InetSocketAddress("localhost", 4567), resourceConfig);

        icarusClient = new IcarusClient.Builder()
                .withHostAddress(serverService.getServerInetAddress().getHostName())
                .withPort(serverService.getServerInetAddress().getPort())
                .withObjectMapper(objectMapper)
                .build(resourceConfig);

        validator = Validation.byDefaultProvider().configure().buildValidatorFactory().getValidator();
    }

    @AfterMethod
    public void teardown() {
        icarusClient.close();
        serverService.stopAsync().awaitTerminated();
    }

    protected Pair<AtomicReference<List<OperationResult<?>>>, AtomicBoolean> performOnRunningServer(final Function<IcarusClient, List<OperationResult<?>>> requestExecutions) {

        final AtomicReference<List<OperationResult<?>>> responseRefs = new AtomicReference<>(new ArrayList<>());

        final AtomicBoolean finished = new AtomicBoolean(false);

        serverService.addListener(new Service.Listener() {
            @Override
            public void running() {
                try {
                    responseRefs.set(requestExecutions.apply(icarusClient));
                } finally {
                    finished.set(true);
                }

            }
        }, MoreExecutors.directExecutor());

        serverService.startAsync();

        return Pair.of(responseRefs, finished);
    }

    protected static final class FailingOperationRequest extends OperationRequest {
        public FailingOperationRequest() {
            this("failing");
        }

        @JsonCreator
        public FailingOperationRequest(@JsonProperty("type") final String type) {
            this.type = type;
        }
    }

    protected static final class FailingOperation extends Operation<FailingOperationRequest> {

        @Inject
        public FailingOperation(@Assisted final FailingOperationRequest request) {
            super(request);
        }

        @JsonCreator
        protected FailingOperation(@JsonProperty("type") final String type,
                                   @JsonProperty("id") final UUID id,
                                   @JsonProperty("creationTime") final Instant creationTime,
                                   @JsonProperty("state") final State state,
                                   @JsonProperty("errors") final List<Error> errors,
                                   @JsonProperty("progress") final float progress,
                                   @JsonProperty("startTime") final Instant startTime) {
            super(type, id, creationTime, state, errors, progress, startTime, new FailingOperationRequest(type));
        }

        @Override
        protected void run0() throws Exception {
            throw new IllegalStateException("this is exception", new RuntimeException("this is some cause!"));
        }
    }
}
