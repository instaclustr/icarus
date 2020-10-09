package com.instaclustr.icarus.operations.icarus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class StopIcarusOperationRequest extends OperationRequest {

    public StopIcarusOperationRequest() {
        this("stop");
    }

    @JsonCreator
    public StopIcarusOperationRequest(@JsonProperty("type") final String type) {
        this.type = "stop";
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("type", type)
                          .toString();
    }
}
