package com.instaclustr.icarus.operations.restart;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.instaclustr.operations.OperationRequest;

public class RestartOperationRequest extends OperationRequest {

    public RestartOperationRequest() {
        this("restart");
    }

    @JsonCreator
    public RestartOperationRequest(@JsonProperty("type") final String type) {
        this.type = type;
    }
}
