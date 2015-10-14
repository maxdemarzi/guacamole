package com.maxdemarzi.guacamole;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

@Path("/service")
public class Service {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @GET
    @Path("/warmup")
    public Response warmUp(@Context GraphDatabaseService db) throws IOException {
        int counter = 0;
        try (Transaction tx = db.beginTx()) {
            for (Node n : GlobalGraphOperations.at(db).getAllNodes()) {
                n.getPropertyKeys();
                for (Relationship relationship : n.getRelationships()) {
                    relationship.getPropertyKeys();
                    relationship.getStartNode();
                }
            }

            for (Relationship relationship : GlobalGraphOperations.at(db).getAllRelationships()) {
                counter++;
                relationship.getPropertyKeys();
                relationship.getNodes();
            }
        }

        Map<String, Integer> results = new HashMap();
        results.put("count", counter);

        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/document/{key}")
    public Response getDocument(@PathParam("key") String key, @Context GraphDatabaseService db) {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                try (Transaction tx = db.beginTx()) {
                    final Node user = db.findNode(Labels.PROFILES, "_key", key);

                    if (user != null) {
                        jg.writeStartObject();
                        for (String key : user.getPropertyKeys()) {
                            jg.writeObjectField(key, user.getProperty(key));
                        }
                        jg.writeEndObject();
                    }
                }
                jg.flush();
                jg.close();
            }
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

    @GET
    @Path("/aggregate")
    public Response getAggregate(@Context GraphDatabaseService db) {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                HashMap<Long, int[]> ageAggregator = new HashMap<>();
                try (Transaction tx = db.beginTx()) {
                    final ResourceIterator<Node> nodes = db.findNodes(Labels.PROFILES);

                    while(nodes.hasNext()) {
                        Node node = nodes.next();
                        Long age = (Long)node.getProperty("AGE", 0);
                        int[] count = ageAggregator.get(age);
                        if (count == null) {
                            ageAggregator.put(age, new int[]{1});
                        } else {
                            count[0]++;
                        }
                    }
                    jg.writeStartArray();
                    for (Map.Entry<Long, int[]> entry : ageAggregator.entrySet()) {
                        jg.writeStartObject();
                        jg.writeObjectField(entry.getKey().toString(), entry.getValue()[0]);
                        jg.writeEndObject();
                    }
                    jg.writeEndArray();
                }
                jg.flush();
                jg.close();
            }
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }

}