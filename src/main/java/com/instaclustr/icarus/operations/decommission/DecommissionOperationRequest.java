package com.instaclustr.icarus.operations.decommission;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class DecommissionOperationRequest extends OperationRequest {

    public Boolean force;

    public DecommissionOperationRequest(final Boolean force) {
        this("decommission", force);
    }

    @JsonCreator
    public DecommissionOperationRequest(@JsonProperty("type") final String type,
                                        @JsonProperty("force") final Boolean force) {
        this.force = force;
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("force", force)
                          .toString();
    }
}
