package com.maxdemarzi.guacamole;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.cursor.NodeItem;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
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
import java.util.*;

@Path("/service")
public class Service {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final GraphDatabaseAPI dbapi;
    private final PathFinder<org.neo4j.graphdb.Path> finder = GraphAlgoFactory.shortestPath(
            PathExpanders.allTypesAndDirections(), 15);

    public Service(@Context GraphDatabaseService db) {
        dbapi = (GraphDatabaseAPI)db;
    }

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
    @Path("/shortest_path/{key1}/{key2}")
    public Response getShortestPath(@PathParam("key1") String key1,
                                    @PathParam("key2") String key2,
                                    @Context GraphDatabaseService db) {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                try (Transaction tx = db.beginTx()) {
                    final Node node1 = db.findNode(Labels.PROFILES, "_key", key1);
                    final Node node2 = db.findNode(Labels.PROFILES, "_key", key2);
                    org.neo4j.graphdb.Path foundPath = finder.findSinglePath(node1, node2);
                    Iterator<Node> nodes = foundPath.nodes().iterator();
                    jg.writeStartObject();
                    jg.writeArrayFieldStart("path");
                    while (nodes.hasNext()){
                        Node node = nodes.next();
                        jg.writeNumber(node.getId());
                    }
                    jg.writeEndArray();
                    jg.writeEndObject();
                }
                jg.flush();
                jg.close();
            }
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
    }


    @GET
    @Path("/neighbors/{key}")
    public Response getNeighbors(@PathParam("key") String key, @Context GraphDatabaseService db) {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                try (Transaction tx = db.beginTx()) {
                    final Node user = db.findNode(Labels.PROFILES, "_key", key);
                    jg.writeStartArray();
                    for (Relationship r : user.getRelationships(Direction.OUTGOING)) {
                        Node neighbor = r.getEndNode();
                        jg.writeStartObject();
                        jg.writeStringField("_key", (String)neighbor.getProperty("_key"));
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

    @GET
    @Path("/neighbors2/{key}")
    public Response getNeighbors2(@PathParam("key") String key, @Context GraphDatabaseService db) {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

                try (Transaction tx = db.beginTx()) {
                    final Node user = db.findNode(Labels.PROFILES, "_key", key);
                    Set neighbors = new HashSet<String>();
                    jg.writeStartArray();
                    for (Relationship r : user.getRelationships(Direction.OUTGOING)) {
                        Node neighbor = r.getEndNode();
                        String key = (String)neighbor.getProperty("_key");
                        if (!neighbors.contains(key)) {
                            neighbors.add(key);
                            jg.writeStartObject();
                            jg.writeStringField("_key", key);
                            jg.writeEndObject();
                        }

                        for (Relationship r2 : neighbor.getRelationships(Direction.OUTGOING)) {
                            Node neighbor2 = r2.getEndNode();
                            String key2 = (String)neighbor2.getProperty("_key");
                            if (!neighbors.contains(key2)) {
                                neighbors.add(key2);
                                jg.writeStartObject();
                                jg.writeStringField("_key", key2);
                                jg.writeEndObject();
                            }
                        }
                    }
                    jg.writeEndArray();
                }
                jg.flush();
                jg.close();
            }
        };

        return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
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
                HashMap<Integer, int[]> ageAggregator = new HashMap<>();
                try (Transaction tx = db.beginTx()) {
                    final ResourceIterator<Node> nodes = db.findNodes(Labels.PROFILES);

                    while(nodes.hasNext()) {
                        Node node = nodes.next();
                        Integer age = (Integer)node.getProperty("AGE", 0);
                        int[] count = ageAggregator.get(age);
                        if (count == null) {
                            ageAggregator.put(age, new int[]{1});
                        } else {
                            count[0]++;
                        }
                    }
                    jg.writeStartArray();
                    for (Map.Entry<Integer, int[]> entry : ageAggregator.entrySet()) {
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

    @GET
    @Path("/aggregate2")
    public Response getAggregate2(@Context GraphDatabaseService db)  {
        StreamingOutput stream = new StreamingOutput() {
            @Override
            public void write(OutputStream os) throws IOException, WebApplicationException {
                JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);
                HashMap<Integer, int[]> ageAggregator = new HashMap<>();
                try (Transaction tx = db.beginTx()) {
                    ThreadToStatementContextBridge ctx = dbapi.getDependencyResolver().resolveDependency(ThreadToStatementContextBridge.class);
                    ReadOperations ops = ctx.get().readOperations();
                    int labelId = ops.labelGetForName(Labels.PROFILES.name());
                    int propertyKey = ops.propertyKeyGetForName("_key");
                    int propertyAge = ops.propertyKeyGetForName("AGE");

                    Cursor<NodeItem> nodes = ops.nodeCursorGetForLabel(propertyKey);
                    while(nodes.next()) {
                        Object ageProperty = nodes.get().getProperty(propertyAge);
                        if ( ageProperty == null) {
                            ageProperty = 0;
                        }
                        int[] count = ageAggregator.get(ageProperty);
                        if (count == null) {
                            ageAggregator.put((Integer)ageProperty, new int[]{1});
                        } else {
                            count[0]++;
                        }
                    }

                    jg.writeStartArray();
                    for (Map.Entry<Integer, int[]> entry : ageAggregator.entrySet()) {
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
