package de.jbellmann.tomcat.cassandra;

import java.nio.ByteBuffer;
import java.util.Arrays;

import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class HectorZwoIT {

    private static final String KEY_SPACE = "TomcatSessionManagerKeySpace";
    private static final String COLUMN_FAMILY = "TomcatSessionManagerColumnFamily";

    private Cluster cluster;

    @Before
    public void setUp() {
        cluster = HFactory.getOrCreateCluster("TomcatSessionManagerCluster", "localhost:9160");
        // columnFamily
        ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(KEY_SPACE, COLUMN_FAMILY,
                ComparatorType.BYTESTYPE);
        // Keyspace
        KeyspaceDefinition ksDef = HFactory.createKeyspaceDefinition(KEY_SPACE,
                "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef));

        KeyspaceDefinition schema = cluster.describeKeyspace(KEY_SPACE);
        if (schema == null) {
            // create and wait
            cluster.addKeyspace(ksDef, true);
        }
    }

    @After
    public void tearDown() {
        // nothing to do
    }

    @Test
    public void testHectorZwo() throws InterruptedException {

        // create an Keyspace
        Keyspace ksp = HFactory.createKeyspace(KEY_SPACE, cluster);
        // create an Template
        ColumnFamilyTemplate<String, Object> template = new ThriftColumnFamilyTemplate<String, Object>(ksp,
                COLUMN_FAMILY, StringSerializer.get(), ObjectSerializer.get());

        //        String uuid = UUID.randomUUID().toString();
        MySession session = MySession.create();
        String uuid = session.getId();
        System.out.println("Session to save is : " + session.toString());

        ColumnFamilyUpdater<String, Object> updater = template.createUpdater(uuid);
        updater.setValue("session", session, ObjectSerializer.get());

        template.update(updater);
        // throttle
        Thread.sleep(2000);

        ColumnFamilyResult<String, Object> columnFamilyResult = template.queryColumns(uuid);
        HColumn<Object, ByteBuffer> result = columnFamilyResult.getColumn("session");
        Assert.assertNotNull(result);
        Object resultObject = ObjectSerializer.get().fromByteBuffer(result.getValue());
        Assert.assertNotNull(resultObject);
        final MySession retrieved = (MySession) resultObject;
        Assert.assertEquals(session.getId(), retrieved.getId());
        System.out.println("Session saved was : " + retrieved);

    }

    @Ignore
    @Test
    public void testOperation() throws InterruptedException {
        Keyspace keyspace = HFactory.createKeyspace(KEY_SPACE, cluster);
        Mutator<Object> mutator = HFactory.createMutator(keyspace, ObjectSerializer.get());
        MySession session = MySession.create();
        System.out.println("Session to save : " + session.getId());
        HColumn<String, Object> column = HFactory.createColumn("session", session, StringSerializer.get(),
                ObjectSerializer.get());
        mutator.insert(session.getId(), COLUMN_FAMILY, column);

        ColumnQuery<String, String, Object> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
                StringSerializer.get(), ObjectSerializer.get());

        // throttle
        Thread.sleep(2000);
        QueryResult<HColumn<String, Object>> queryResult = query.setKey(session.getId()).setName("session")
                .setColumnFamily(COLUMN_FAMILY).execute();

        HColumn<String, Object> retrieved = queryResult.get();
        Object value = retrieved.getValue();
        Assert.assertNotNull(value);
        Assert.assertEquals(session.getId(), ((MySession) value).getId());
        System.out.println("Session retrieved : " + value.toString());
    }
}
