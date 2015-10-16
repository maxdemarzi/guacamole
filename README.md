# Guacamole
Example Neo4j Extension Testing 

# Instructions

1. Build it:

        mvn clean package

2. Copy target/guacamole-1.0.jar to the plugins/ directory of your Neo4j server.

3. Configure Neo4j by adding a line to conf/neo4j-server.properties:

        org.neo4j.server.thirdparty_jaxrs_classes=com.maxdemarzi.guacamole=/v1
        
4. Start Neo4j server.

5. Create some test data:

        CREATE (:PROFILES {_key:'u1', name:'Max', AGE:36})
        CREATE (:PROFILES {_key:'u2', name:'James', AGE:36})
        CREATE (:PROFILES {_key:'u3', name:'Tim', AGE:37})
        
6. Try it:
        
        :GET /v1/service/warmup
        :GET /v1/service/document/u1
        :GET /v1/service/aggregate
        :GET /v1/service/aggregate2
        :GET /v1/service/shortest_path/u1/u2
        :GET /v1/service/neighbors/u1
        :GET /v1/service/neighbors2/u1

7. Test it.