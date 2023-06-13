package com.instaclustr.icarus.operations.flush;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

public class FlushOperationRequest extends OperationRequest {

    public String keyspace;

    public Set<String> tables;

    public FlushOperationRequest(final String keyspace, final Set<String> tables) {
        this("flush", keyspace, tables);
    }

    @JsonCreator
    public FlushOperationRequest(@JsonProperty("type") final String type,
                                 @JsonProperty("keyspace") final String keyspace,
                                 @JsonProperty("tables") final Set<String> tables) {
        this.keyspace = keyspace;
        this.tables = tables;
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("keyspace", keyspace)
                          .add("tables", tables)
                          .toString();
    }
}
