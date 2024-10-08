package com.instaclustr.icarus.operations.cleanup;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.instaclustr.operations.OperationRequest;

/**
 * <pre>
 * {@code
 * $ nodetool help cleanup
 * NAME
 *         nodetool cleanup - Triggers the immediate cleanup of keys no longer
 *         belonging to a node. By default, clean all keyspaces
 *
 * SYNOPSIS
 *         nodetool [(-h <host> | --host <host>)] [(-p <port> | --port <port>)]
 *                 [(-pw <password> | --password <password>)]
 *                 [(-pwf <passwordFilePath> | --password-file <passwordFilePath>)]
 *                 [(-u <username> | --username <username>)] cleanup
 *                 [(-j <jobs> | --jobs <jobs>)] [--] [<keyspace> <tables>...]
 *
 * OPTIONS
 *         -h <host>, --host <host>
 *             Node hostname or ip address
 *
 *         -j <jobs>, --jobs <jobs>
 *             Number of sstables to cleanup simultanously, set to 0 to use all
 *             available compaction threads
 *
 *         -p <port>, --port <port>
 *             Remote jmx agent port number
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
 *         [<keyspace> <tables>...]
 *             The keyspace followed by one or many tables
 * }</pre>
 */
public class CleanupOperationRequest extends OperationRequest {

    public final String keyspace;

    public final Set<String> tables;

    public final int jobs;

    public CleanupOperationRequest(final String keyspace, final Set<String> tables, final int jobs) {
        this("cleanup", keyspace, tables, jobs);
    }

    @JsonCreator
    public CleanupOperationRequest(@JsonProperty("type") final String type,
                                   @JsonProperty("keyspace") final String keyspace,
                                   @JsonProperty("tables") final Set<String> tables,
                                   @JsonProperty("jobs") final int jobs) {
        this.jobs = jobs;
        this.keyspace = keyspace;
        this.tables = tables;
        this.type = type;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("tables", tables)
                          .add("keyspace", keyspace)
                          .add("jobs", jobs)
                          .toString();
    }
}
