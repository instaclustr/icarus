package com.instaclustr.cassandra.sidecar.operations.drain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.instaclustr.operations.OperationRequest;

public class DrainOperationRequest extends OperationRequest {

    public DrainOperationRequest() {
        this("drain");
    }

    @JsonCreator
    public DrainOperationRequest(@JsonProperty("type") final String type) {
        this.type = type;
    }
}
