package com.instaclustr.cassandra.sidecar.operations.decommission;


import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.base.MoreObjects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.instaclustr.operations.OperationRequest;

public class DecommissionOperationRequest extends OperationRequest {

    public Boolean force;

    @JsonCreator
    public DecommissionOperationRequest(@JsonProperty("force") final Boolean force) {
        this.force = force;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("force", force)
                          .toString();
    }
}
