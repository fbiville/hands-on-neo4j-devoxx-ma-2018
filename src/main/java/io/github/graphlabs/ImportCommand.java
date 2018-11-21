package io.github.graphlabs;

import net.biville.florent.repl.console.commands.Command;
import net.biville.florent.repl.exercises.TraineeSession;
import net.biville.florent.repl.graph.cypher.CypherQueryExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Function.identity;

public class ImportCommand implements Command {

    private static final int BATCH_SIZE = 1_000;

    private final CypherQueryExecutor executor;

    public ImportCommand(CypherQueryExecutor executor) {
        this.executor = executor;
    }

    @Override
    public String help() {
        return String.format("%s - imports dataset (this command is idempotent but MAY TAKE SOME TIME TO COMPLETE)", this.name());
    }

    @Override
    public String name() {
        return Command.PREFIX + "import";
    }

    @Override
    public void accept(TraineeSession traineeSession, String unused) {
        traineeSession.reset();
        executeInSingleTx("/cineasts-indices.cypher");
        executeInBatch("/cineasts-nodes.cypher", BaseStream::parallel);
        executeInBatch("/cineasts-rels.cypher", identity());
        executeInSingleTx("/cineasts-cleanup.cypher");
        executeInSingleTx("/cineasts-indices-cleanup.cypher");
    }

    private void executeInBatch(String resource, Function<Stream<String>, Stream<String>> transformStream) {
        try (BufferedReader reader = reader(resource)) {
            AtomicInteger counter = new AtomicInteger(0);
            transformStream.apply(reader.lines())
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / BATCH_SIZE))
                    .values()
                    .forEach(batch -> {
                        executor.commit(tx -> {
                            batch.forEach(tx::run);
                        });
                    });

        } catch (IOException ignored) {
        }
    }

    private void executeInSingleTx(String s) {
        try (BufferedReader reader = reader(s)) {
            executor.commit((tx) -> {
                reader.lines().forEach(tx::run);
            });
        } catch (IOException ignored) {
        }
    }

    private BufferedReader reader(String resource) {
        return new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(resource)));
    }
}
