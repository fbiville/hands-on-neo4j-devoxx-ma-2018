package io.github.graphlabs;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import net.biville.florent.repl.Neo4jRepl;
import net.biville.florent.repl.graph.ReplConfiguration;
import java.util.logging.LogManager;

public class Main {

    @Parameter(names = {"-b", "--bolt-uri"}, description = "Neo4j Bolt URI")
    private String boltUri = "bolt://localhost:7687";

    @Parameter(names = {"-u",
            "--username"}, description = "Neo4j User name")
    private String username = "neo4j";

    @Parameter(names = {"-p",
            "--password"}, description = "Neo4j password", required = true, password = true)
    private String password;

    @Parameter(names = {"-h", "--help"}, description = "Help", help = true)
    private boolean help;

    public static void main(String[] args) {
        LogManager.getLogManager().reset();
        Main main = new Main();
        JCommander cli = parseCommandLineArguments(args, main);
        if (main.help) {
            cli.usage();
            return;
        }

        new Neo4jRepl(main.getConfiguration(main.getClass().getPackage().getName()))
                .get()
                .start();
    }

    private static JCommander parseCommandLineArguments(String[] args, Main main) {
        JCommander command = getJCommander(args, main);
        command.setProgramName("hands-on-neo4j-devoxx-ma-2018");
        return command;
    }

    private static JCommander getJCommander(String[] args, Main main) {
        try {
            JCommander jCommander = new JCommander(main);
            jCommander.parse(args);
            return jCommander;
        } catch (ParameterException pe) {
            main.help = true;
            return new JCommander(main);
        }
    }

    private ReplConfiguration getConfiguration(String packageToScan) {
        return new ReplConfiguration(boltUri, username, password, packageToScan);
    }
}
