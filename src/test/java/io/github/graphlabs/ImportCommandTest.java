package io.github.graphlabs;

import net.biville.florent.repl.exercises.TraineeSession;
import net.biville.florent.repl.graph.ReplConfiguration;
import net.biville.florent.repl.graph.cypher.CypherQueryExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.logging.LogManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class ImportCommandTest {

    static {
        LogManager.getLogManager().reset();
    }

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Before
    public void prepare() {
        CypherQueryExecutor executor = new CypherQueryExecutor(new ReplConfiguration(neo4j.boltURI()));
        ImportCommand importCommand = new ImportCommand(executor);
        importCommand.accept(mock(TraineeSession.class), "any string");
    }

    @Test
    public void imports_multiline_statements() {
        GraphDatabaseService graphDatabaseService = neo4j.getGraphDatabaseService();
        try (Transaction ignored = graphDatabaseService.beginTx();
             Result result = graphDatabaseService.execute("MATCH (n) RETURN n.name AS name")) {

            assertThat(result.columnAs("name")).containsOnly("Jeff Goldblum", "Mary McDonnell");
        }
    }
}