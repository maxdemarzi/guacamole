package com.maxdemarzi.guacamole;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.function.Function;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.assertTrue;

public class GetAggregateTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(new Function<GraphDatabaseService, Void>() {
                @Override
                public Void apply(GraphDatabaseService db) throws RuntimeException {
                    try (Transaction tx = db.beginTx()) {
                        Node one = createUser(db, "u1", "Max", 36);
                        Node two = createUser(db, "u2", "James", 36);
                        Node three = createUser(db, "u3", "Tim", 37);
                        tx.success();
                    }
                    return null;
                }
            })
            .withExtension("/v1", Service.class);

    private Node createUser(GraphDatabaseService db, String key, String name, Integer age) {
        Node one = db.createNode(DynamicLabel.label("PROFILES"));
        one.setProperty("_key", key);
        one.setProperty("name", name);
        one.setProperty("AGE", age);
        return one;
    }

    @Test
    public void shouldGetAggregate() throws IOException {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/aggregate").toString());
        ArrayList actual = response.content();
        System.out.println(actual);
        HashSet expectedSet = new HashSet<>(expected);
        HashSet actualSet = new HashSet<>(actual);

        assertTrue(actualSet.equals(expectedSet));
    }

    @Test
    public void shouldGetAggregate2() throws IOException {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/aggregate2").toString());
        ArrayList actual = response.content();
        System.out.println(actual);
        HashSet expectedSet = new HashSet<>(expected);
        HashSet actualSet = new HashSet<>(actual);

        assertTrue(actualSet.equals(expectedSet));
    }

    private static final ArrayList expected = new ArrayList() {{
        add(new HashMap<String, Object>() {{
            put("36", 2);
        }});
        add(new HashMap<String, Object>() {{
            put("37", 1);
        }});
    }};
}
