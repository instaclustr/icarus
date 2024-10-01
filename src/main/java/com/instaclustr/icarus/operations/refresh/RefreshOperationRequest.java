package com.instaclustr.icarus.operations.refresh;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

/**
 * <pre>
 * {@code
 * NAME
 *         nodetool refresh - Load newly placed SSTables to the system without
 *         restart
 *
 * SYNOPSIS
 *         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
 *                 [(-pp | --print-port)] [(-pw <password> | --password <password>)]
 *                 [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
 *                 [(-u <username> | --username <username>)] refresh [--] <keyspace>
 *                 <table>
 *
 * OPTIONS
 *         -h <host>, --host <host>
 *             Node hostname or ip address
 *
 *         -p <port>, --port <port>
 *             Remote jmx agent port number
 *
 *         -pp, --print-port
 *             Operate in 4.0 mode with hosts disambiguated by port number
 *
 *         -pw <password>, --password <password>
 *             Remote jmx agent password
 *
 *         -pwf <passwordFilePath>, --password-file <passwordFilePath>
 *             Path to the JMX password file
 *
 *         -u <username>, --username <username>
 *             Remote jmx agent username
 *
 *         --
 *             This option can be used to separate command-line options from the
 *             list of argument, (useful when arguments might be mistaken for
 *             command-line options
 *
 *         <keyspace> <table>
 *             The keyspace and table name
 * }</pre>
 */
public class RefreshOperationRequest extends OperationRequest {

    public final String keyspace;

    public final String table;

    public RefreshOperationRequest(final String keyspace, final String table) {
        this("refresh", keyspace, table);
    }

    @JsonCreator
    public RefreshOperationRequest(@JsonProperty("type") final String type,
                                   @JsonProperty("keyspace") final String keyspace,
                                   @JsonProperty("table") final String table) {
        this.keyspace = keyspace;
        this.table = table;
        this.type = "refresh";
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("keyspace", keyspace)
                          .add("table", table)
                          .toString();
    }
}
