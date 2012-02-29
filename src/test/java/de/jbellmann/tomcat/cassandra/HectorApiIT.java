package de.jbellmann.tomcat.cassandra;

import java.util.Arrays;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class HectorApiIT {

    private static final String KEYSPACE_NAME = "Keyspace1";
    private static final String COLUMN_FAMILY_NAME = "Standard";

    private Cluster cluster;
    //    private KeyspaceDefinition keyspaceDefinition;
    private ColumnFamilyDefinition cfDef;

    //    private Keyspace keyspace;
    //    private ColumnFamilyDefinition cfd;

    @Before
    public void setUp() {
        cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        //        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition("Keyspace1");
        //        String schemaId = cluster.addKeyspace(keyspaceDefinition, true);
        //        System.out.println("SchemaId created : " + schemaId);
        //        System.out.println("KeyspaceDefinitionName : " + keyspaceDefinition.getName());
        //        keyspace = HFactory.createKeyspace(keyspaceDefinition.getName(), cluster);
        //        cfd = HFactory.createColumnFamilyDefinition(keyspaceDefinition.getName(), "Standard");
        //
        BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
        columnFamilyDefinition.setKeyspaceName(KEYSPACE_NAME);
        columnFamilyDefinition.setName(COLUMN_FAMILY_NAME);

        cfDef = new ThriftCfDef(columnFamilyDefinition);

        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(KEYSPACE_NAME,
                "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef));

        cluster.addKeyspace(keyspaceDefinition, true);

    }

    @After
    public void tearDown() {
        cluster.getConnectionManager().shutdown();
    }

    @Test
    public void testHectorApi() throws InterruptedException {
        Mutator<Object> mutator = HFactory.createMutator(HFactory.createKeyspace(KEYSPACE_NAME, cluster),
                ObjectSerializer.get());
        MySession session = MySession.create();
        String id = session.getId();
        System.out.println("Session created with id : " + id);
        try {
            MutationResult mutationResult = mutator.insert(id, cfDef.getName(),
                    HFactory.createColumn("sessionId", session, StringSerializer.get(), ObjectSerializer.get()));
            System.out.println("Session saved with id : " + id);

            // Throttle
            Thread.sleep(5000);

            // Retrieve back
            ColumnQuery<String, String, Object> query = HFactory.createColumnQuery(
                    HFactory.createKeyspace(KEYSPACE_NAME, cluster), StringSerializer.get(), StringSerializer.get(),
                    ObjectSerializer.get());
            query.setColumnFamily(cfDef.getName()).setKey(id).setName("sessionId");
            QueryResult<HColumn<String, Object>> result = query.execute();

            //
            System.out.println(result.get());
        } catch (HectorException e) {
            e.printStackTrace();
        }
    }
}
