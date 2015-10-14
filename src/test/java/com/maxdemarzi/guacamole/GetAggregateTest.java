package com.maxdemarzi.guacamole;

import org.junit.Rule;
import org.junit.Test;
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
            .withFixture( CYPHER_STATEMENT )
            .withExtension("/v1", Service.class);

    @Test
    public void shouldGetAggregate() throws IOException {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/aggregate").toString());
        ArrayList actual = response.content();
        System.out.println(actual);
        HashSet expectedSet = new HashSet<>(expected);
        HashSet actualSet = new HashSet<>(actual);

        assertTrue(actualSet.equals(expectedSet));
    }

    private static final String CYPHER_STATEMENT =
            new StringBuilder()
                    .append("CREATE (:PROFILES {_key:'u1', name:'Max', AGE:36}) ")
                    .append("CREATE (:PROFILES {_key:'u2', name:'James', AGE:36}) ")
                    .append("CREATE (:PROFILES {_key:'u3', name:'Tim', AGE:37}) ")
                    .toString();

    private static final ArrayList expected = new ArrayList() {{
        add(new HashMap<String, Object>() {{
            put("36", 2);
        }});
        add(new HashMap<String, Object>() {{
            put("37", 1);
        }});
    }};
}
