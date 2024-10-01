package com.instaclustr.operations;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.instaclustr.icarus.operations.decommission.DecommissionOperationRequest;
import com.instaclustr.sidecar.operations.OperationsResource;
import com.instaclustr.threading.ExecutorsModule;
import jakarta.ws.rs.core.Response;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class IcarusTest {

    @Inject
    OperationsResource operationsResource;

    private static final class TestingDecommissionOperation extends Operation<DecommissionOperationRequest> {

        private final CountDownLatch countDownLatch;

        protected TestingDecommissionOperation(DecommissionOperationRequest request, CountDownLatch countDownLatch) {
            super(request);
            this.countDownLatch = countDownLatch;
        }

        @Override
        protected void run0() throws Exception {
            countDownLatch.await();
        }
    }

    private final CountDownLatch operationCountDownLatch = new CountDownLatch(1);

    @BeforeTest
    public void setup() {

        final Map<Class<? extends OperationRequest>, OperationFactory> typeMap = new HashMap<Class<? extends OperationRequest>, OperationFactory>() {{
            put(DecommissionOperationRequest.class, (OperationFactory<DecommissionOperationRequest>) request -> new TestingDecommissionOperation(request, operationCountDownLatch));
        }};

        final Map<String, Class<? extends OperationRequest>> typeMappings = new HashMap<String, Class<? extends OperationRequest>>() {{
            put("decommission", DecommissionOperationRequest.class);
        }};

        final TypeLiteral<Map<Class<? extends OperationRequest>, OperationFactory>> classType = new TypeLiteral<Map<Class<? extends OperationRequest>, OperationFactory>>() {
        };

        final TypeLiteral<Map<String, Class<? extends OperationRequest>>> map = new TypeLiteral<Map<String, Class<? extends OperationRequest>>>() {
        };

        final Injector injector = Guice.createInjector(
                new OperationsModule(3600),
                new ExecutorsModule(),
                new AbstractModule() {
                    @Override
                    protected void configure() {
                        bind(classType).toInstance(typeMap);
                        bind(map).toInstance(typeMappings);
                    }
                });

        injector.injectMembers(this);
    }

    @Test
    void testOperationsService() throws InterruptedException {

        // create operation
        final Response newOperation = operationsResource.createNewOperation(new DecommissionOperationRequest(false));

        final UUID operationID = getOperationUUID(newOperation);
        assertNotNull(operationID);

        // get operation by its id and get all operations
        Operation submittedOperation = operationsResource.getOperationById(operationID);
        assertNotNull(operationID);

        final Collection<Operation<?>> allOperations = operationsResource.getOperations(ImmutableSet.of(), ImmutableSet.of());
        assertFalse(allOperations.isEmpty());

        // check that this is operation we wanted and it is in running state
        assertTrue(allOperations.stream().anyMatch(op -> op.id.equals(operationID)));
        assertEquals(submittedOperation.state, Operation.State.RUNNING);

        // let operation finish
        operationCountDownLatch.countDown();

        Thread.sleep(1000); // sleep here cause this test proceeds but it is not finished yet

        Operation finishedOperation = operationsResource.getOperationById(operationID);
        assertNotNull(operationID);

        // check it is completed and completed status is returned from all operations endpoint too
        assertEquals(finishedOperation.state, Operation.State.COMPLETED);

        final Collection<Operation<?>> allOperationsAfterFinish = operationsResource.getOperations(ImmutableSet.of(), ImmutableSet.of());
        assertFalse(allOperationsAfterFinish.isEmpty());

        assertTrue(allOperations.stream().anyMatch(op -> op.id.equals(operationID) && op.state == Operation.State.COMPLETED));
    }

    private UUID getOperationUUID(final Response response) {
        final URI location = response.getLocation();

        assertNotNull(location);

        final String[] paths = location.getPath().split("/");

        assertEquals(paths.length, 3);

        return UUID.fromString(paths[2]);
    }
}
