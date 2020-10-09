package com.instaclustr.icarus;

import com.instaclustr.esop.cli.Esop;
import com.instaclustr.picocli.CLIApplication;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@Command(
        mixinStandardHelpOptions = true,
        subcommands = {Esop.class, Icarus.class},
        versionProvider = IcarusAggregationCommand.class,
        usageHelpWidth = 128
)
public class IcarusAggregationCommand extends CLIApplication implements Runnable {

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        main(args, true);
    }

    public static void main(String[] args, boolean exit) {
        int exitCode = execute(new CommandLine(new IcarusAggregationCommand()), args);

        if (exit) {
            System.exit(exitCode);
        }
    }

    @Override
    public String getImplementationTitle() {
        return "sidecar-aggregator";
    }

    @Override
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required sub-command.");
    }
}
