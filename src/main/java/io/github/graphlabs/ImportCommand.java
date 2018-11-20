package io.github.graphlabs;

import net.biville.florent.repl.console.commands.Command;
import net.biville.florent.repl.exercises.TraineeSession;
import net.biville.florent.repl.graph.cypher.CypherQueryExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ImportCommand implements Command {

    private static final int BATCH_SIZE = 5_000;

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
        insertIndices();
        insertData();
        cleanUpIndices();
    }

    private void insertIndices() {
        try (BufferedReader reader = reader("/indices.cypher")) {
            executor.commit((tx) -> {
                reader.lines().forEach(tx::run);
            });
        } catch (IOException ignored) {
        }
    }

    private void insertData() {
        try (BufferedReader reader = reader("/cineasts.cypher")) {
            AtomicInteger counter = new AtomicInteger(0);
            reader.lines()
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

    private void cleanUpIndices() {
        try (BufferedReader reader = reader("/indices-cleanup.cypher")) {
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

class Tuple2<U, V> {
    private final U v1;
    private final V v2;

    public Tuple2(U v1, V v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public U getV1() {
        return v1;
    }

    public V getV2() {
        return v2;
    }
}
