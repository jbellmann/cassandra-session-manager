package de.jbellmann.tomcat.cassandra;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class ThriftTest {

    private TTransport transport;
    private TProtocol protocol;
    private TSocket socket;
    private Cassandra.Client client;

    @Before
    public void setUp() throws TTransportException {
        socket = new TSocket("localhost", 9160);
        transport = new TFramedTransport(socket);
        protocol = new TBinaryProtocol(transport);
        transport.open();
        client = new Cassandra.Client(protocol);
    }

    @After
    public void tearDown() {
        transport.close();
        socket.close();
    }

    @Test
    public void testThriftConnection() {

    }

    protected KsDef getKeyspaceDefinition(String keyspaceName, String columnfamilyName) {

        return null;
    }

}
