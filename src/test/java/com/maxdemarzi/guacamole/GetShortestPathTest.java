package com.maxdemarzi.guacamole;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class GetShortestPathTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withFixture( CYPHER_STATEMENT )
            .withExtension("/v1", Service.class);

    @Test
    public void shouldGetShortestPath() throws IOException {
        HTTP.Response response = HTTP.GET(neo4j.httpURI().resolve("/v1/service/shortest_path/u1/u2").toString());
        HashMap actual = response.content();
        assertTrue(actual.equals(expected));
    }

    private static final String CYPHER_STATEMENT =
            new StringBuilder()
                    .append("CREATE (user1:PROFILES {_key:'u1', name:'Max'}) ")
                    .append("CREATE (user2:PROFILES {_key:'u2', name:'Tom'}) ")
                    .append("CREATE (user1)-[:RELATIONS]->(user2) ")
                    .toString();

    private static final HashMap expected = new HashMap<String,Object>(){{
        put("path", new ArrayList(){{add(0); add(1);}});
    }};
}