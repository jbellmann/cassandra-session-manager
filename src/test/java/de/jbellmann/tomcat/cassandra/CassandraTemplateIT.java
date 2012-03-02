package de.jbellmann.tomcat.cassandra;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraTemplateIT {

    private static final String CF_NAME = "CassandraTemplateColumnFamily";
    private static final String KS_NAME = "CassandraTemplateKeyspace";

    private static final Map<String, String> STRATEGY_OPTIONS = new HashMap<String, String>();

    private TTransport transport;
    private TProtocol protocol;
    private TSocket socket;

    @Before
    public void setUp() throws TTransportException {
        STRATEGY_OPTIONS.put("replication_factor", "1");
        STRATEGY_OPTIONS.put("rows_cached", "300");
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
    public void testSetCreationTime() {
        CassandraTemplate template = new TestCassandraTemplate();
        for (int i = 0; i < 20; i++) {
            template.setCreationTime(createRandomSessionId(), System.currentTimeMillis());
        }
        List<String> sessionIdList = template.findSessionKeys();
        System.out.println("Found sessionIds : " + sessionIdList.size());
        for (String sessionId : sessionIdList) {
            long creationTime = template.getCreationTime(sessionId);
            System.out.println("Session : " + sessionId + " -- creationTime : " + creationTime);
        }
    }

    protected String createRandomSessionId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    class TestCassandraTemplate extends CassandraTemplate {

        ColumnParent columnParent = null;

        @Override
        protected Client getClient() {
            Cassandra.Client client = new Cassandra.Client(protocol);
            if (schemaUpdateRequired(client)) {
                try {
                    client.system_add_keyspace(createKeyspaceDefinition());
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (SchemaDisagreementException e) {
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
            try {
                client.set_keyspace(KS_NAME);
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
            return client;
        }

        protected boolean schemaUpdateRequired(Client client) {
            try {
                KsDef keyspaceDefinition = client.describe_keyspace(KS_NAME);
                if (keyspaceDefinition == null) {
                    return true;
                } else {
                    return false;
                }
            } catch (NotFoundException e) {
                return true;
            } catch (InvalidRequestException e) {
                throw new RuntimeException(e);
            } catch (TException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected ColumnParent getColumnParent() {
            if (columnParent != null) {
                return columnParent;
            } else {
                ColumnParent columnParent = new ColumnParent();
                columnParent.setColumn_family(CF_NAME);
                this.columnParent = columnParent;
                return this.columnParent;
            }
        }

        @Override
        protected ColumnPath getColumnPath(String columnName) {
            ColumnPath columnPath = new ColumnPath();
            columnPath.setColumn_family(CF_NAME);
            columnPath.setColumn(stringSerializer.toByteBuffer(columnName));
            return columnPath;
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

    }

}
