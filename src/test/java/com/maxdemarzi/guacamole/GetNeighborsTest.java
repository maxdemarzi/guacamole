package com.maxdemarzi.guacamole;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class GetNeighborsTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture(CYPHER_STATEMENT)
            .withExtension("/v1", Service.class);

    @Test
    public void shouldGetNeighbors() throws IOException {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/neighbors/u1").toString());
        ArrayList actual = response.content();
        assertThat(actual, is(expected));
    }

    private static final String CYPHER_STATEMENT =
            new StringBuilder()
                    .append("CREATE (user1:PROFILES {_key:'u1', name:'Max'}) ")
                    .append("CREATE (user2:PROFILES {_key:'u2', name:'Tom'}) ")
                    .append("CREATE (user3:PROFILES {_key:'u3', name:'Jim'}) ")
                    .append("CREATE (user1)-[:RELATIONS]->(user2) ")
                    .append("CREATE (user1)-[:RELATIONS]->(user3) ")
                    .toString();

    private static final ArrayList expected = new ArrayList<HashMap<String, Object>>() {{
        add(new HashMap<String, Object>() {{ put("_key", "u3"); }});
        add(new HashMap<String, Object>() {{ put("_key", "u2"); }});
    }};
}
