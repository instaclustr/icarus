package com.instaclustr.cassandra.sidecar.operations.restart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.instaclustr.operations.OperationRequest;

public class RestartSidecarOperationRequest extends OperationRequest {

    public RestartSidecarOperationRequest() {
        this("restart-sidecar");
    }

    @JsonCreator
    public RestartSidecarOperationRequest(@JsonProperty("type") final String type) {
        this.type = type;
    }
}
