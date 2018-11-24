package io.github.graphlabs;

import net.biville.florent.repl.console.commands.Command;
import net.biville.florent.repl.exercises.TraineeSession;
import net.biville.florent.repl.graph.cypher.CypherQueryExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class ImportCommand implements Command {

    private final CypherQueryExecutor queryExecutor;

    private final ExecutorService executorService;

    public ImportCommand(CypherQueryExecutor queryExecutor) {
        this.queryExecutor = queryExecutor;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
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
        concurrentlyImportInBatch("/cineasts-nodes.cypher", 200);
        seriallyImportInBatch("/cineasts-rels.cypher", 1000);
        executeInSingleTx("/cineasts-cleanup.cypher");
        executeInSingleTx("/cineasts-indices-cleanup.cypher");
    }

    private void concurrentlyImportInBatch(String resource, int batchSize) {
        try (BufferedReader reader = reader(resource)) {

            AtomicInteger counter = new AtomicInteger(0);
            Collection<List<String>> statementBatches = reader.lines().parallel()
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / batchSize))
                    .values();
            CountDownLatch latch = new CountDownLatch(counter.get());
            statementBatches
                    .forEach(batch -> executorService.execute(() -> queryExecutor.commit(tx -> {
                        batch.forEach(statement -> {
                            latch.countDown();
                            tx.run(statement);
                        });
                    })));
            latch.await();

        } catch (IOException | InterruptedException ignored) {
        }
    }

    private void seriallyImportInBatch(String resource, int batchSize) {
        try (BufferedReader reader = reader(resource)) {
            AtomicInteger counter = new AtomicInteger(0);
            reader.lines()
                    .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / batchSize))
                    .values()
                    .forEach(batch -> queryExecutor.commit(tx -> {
                        batch.forEach(tx::run);
                    }));

        } catch (IOException ignored) {
        }
    }

    private void executeInSingleTx(String s) {
        try (BufferedReader reader = reader(s)) {
            queryExecutor.commit((tx) -> {
                reader.lines().forEach(tx::run);
            });
        } catch (IOException ignored) {
        }
    }

    private BufferedReader reader(String resource) {
        return new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(resource)));
    }
}
