package com.instaclustr.cassandra.sidecar.operations.sidecar;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class StopSidecarOperationRequest extends OperationRequest {

    public StopSidecarOperationRequest() {
        this("stop");
    }

    @JsonCreator
    public StopSidecarOperationRequest(@JsonProperty("type") final String type) {
        this.type = "stop";
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("type", type)
                          .toString();
    }
}
