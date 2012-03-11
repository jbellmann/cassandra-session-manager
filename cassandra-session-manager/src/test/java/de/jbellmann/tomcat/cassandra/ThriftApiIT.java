package de.jbellmann.tomcat.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ThriftApiIT {

    private static final String CF_NAME = "Sessions";
    private static final String KS_NAME = "SessionManager";
    private static final String COL_NAME = "Session";
    private static final Map<String, String> STRATEGY_OPTIONS = new HashMap<String, String>();

    private TTransport transport;
    private TProtocol protocol;
    private TSocket socket;

    @Before
    public void setUp() throws TTransportException {
        STRATEGY_OPTIONS.put("replication_factor", "1");
        socket = new TSocket("localhost", 9160);
        transport = new TFramedTransport(socket);
        protocol = new TBinaryProtocol(transport);
        transport.open();
    }

    @After
    public void tearDown() {
        transport.close();
        socket.close();
    }

    @Test
    public void testThrift() throws InvalidRequestException, SchemaDisagreementException, TException, UnavailableException, TimedOutException, UnsupportedEncodingException, NotFoundException, InterruptedException {
        Cassandra.Client client = createClient();
        client.system_add_keyspace(createKeyspaceDefinition());
        client.set_keyspace(KS_NAME);
        // used to insert
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CF_NAME);
        // specify column
        Column column = new Column();
        column.setName(COL_NAME.getBytes("UTF-8"));
        column.setValue("INSERTED_VALUE".getBytes("UTF-8"));
        column.setTimestamp(System.currentTimeMillis());
        //        column.setTimestampIsSet(true);
        byte[] uuid = UUID.randomUUID().toString().getBytes("UTF-8");
        // insert
        client.insert(ByteBuffer.wrap(uuid), columnParent, column, ConsistencyLevel.ONE);
        //throttle
        Thread.sleep(1000);
        //used to read
        ColumnPath columnPath = new ColumnPath();
        columnPath.setColumn_family(CF_NAME);
        columnPath.setColumn(COL_NAME.getBytes("UTF-8"));

        ColumnOrSuperColumn resultColumn = client.get(ByteBuffer.wrap(uuid), columnPath, ConsistencyLevel.ONE);
        Assert.assertNotNull(resultColumn.getColumn());
        Assert.assertNotNull(resultColumn.getColumn().getValue());
        String result = new String(resultColumn.getColumn().getValue());
        // verify
        Assert.assertNotNull(result);
        Assert.assertEquals("INSERTED_VALUE", result);

    }

    protected KsDef createKeyspaceDefinition() {

        List<CfDef> columnFamilies = new ArrayList<CfDef>();
        CfDef cfDef = new CfDef(KS_NAME, CF_NAME);
        columnFamilies.add(cfDef);

        KsDef ksDef = new KsDef(KS_NAME, "org.apache.cassandra.locator.SimpleStrategy", columnFamilies);
        ksDef.setStrategy_options(STRATEGY_OPTIONS);
        //        ksDef.setStrategy_classIsSet(true);
        //        ksDef.setStrategy_optionsIsSet(true);
        return ksDef;
    }

    protected Cassandra.Client createClient() {
        return new Cassandra.Client(protocol);
    }

}
