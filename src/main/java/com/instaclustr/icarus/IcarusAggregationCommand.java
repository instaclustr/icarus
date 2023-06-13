package com.instaclustr.icarus;

import com.instaclustr.esop.cli.Esop;
import com.instaclustr.picocli.CLIApplication;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Optional;
import java.util.Properties;

@Command(
        mixinStandardHelpOptions = true,
        subcommands = {Esop.class, Icarus.class},
        versionProvider = IcarusAggregationCommand.IcarusVersionParser.class,
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
    public void run() {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required sub-command.");
    }

    @Override
    public String title() {
        return "icarus-aggregator";
    }

    public static class IcarusVersionParser implements CommandLine.IVersionProvider {
        public IcarusVersionParser() {
        }

        public static String[] parse(String title) throws IOException {
            Enumeration<URL> resources = CommandLine.class.getClassLoader().getResources("git.properties");
            Optional<String> implementationVersion = Optional.empty();
            Optional<String> buildTime = Optional.empty();
            Optional<String> gitCommit = Optional.empty();

            while(resources.hasMoreElements()) {
                URL url = (URL)resources.nextElement();
                Properties properties = new Properties();
                properties.load(url.openStream());

                if (properties.getProperty("git.build.time") != null) {
                    implementationVersion = Optional.ofNullable(properties.getProperty("git.build.version"));
                    buildTime = Optional.ofNullable(properties.getProperty("git.build.time"));
                    gitCommit = Optional.ofNullable(properties.getProperty("git.commit.id"));
                }
            }

            return new String[]{
                    String.format("%s %s", title, implementationVersion.orElse("development build")),
                    String.format("Build time: %s", buildTime.orElse("unknown")),
                    String.format("Git commit: %s", gitCommit.orElse("unknown"))
            };
        }

        @Override
        public String[] getVersion() throws Exception {
            return IcarusVersionParser.parse("instaclustr-icarus");
        }
    }
}
