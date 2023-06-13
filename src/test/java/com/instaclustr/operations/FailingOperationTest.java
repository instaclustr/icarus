package com.instaclustr.operations;

import static com.google.common.collect.ImmutableList.of;
import static com.instaclustr.operations.Operation.State.FAILED;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.instaclustr.icarus.rest.IcarusClient;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

public class FailingOperationTest extends AbstractIcarusTest {

    @Test
    public void operationFailingTest() throws JsonProcessingException {
        final Function<IcarusClient, List<IcarusClient.OperationResult<?>>> requests = client -> of(client.performOperationSubmission(new FailingOperationRequest()));
        final Pair<AtomicReference<List<IcarusClient.OperationResult<?>>>, AtomicBoolean> result = performOnRunningServer(requests);

        await().atMost(1, MINUTES).until(() -> result.getRight().get());
        await().until(() -> icarusClient.getOperations().stream().allMatch(operation -> operation.state == FAILED));


        Operation<?> operation = icarusClient.getOperation(result.getKey().get().get(0).operation.id);

        logger.info(objectMapper.writeValueAsString(operation));
    }
}
