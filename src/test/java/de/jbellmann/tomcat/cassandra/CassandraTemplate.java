package de.jbellmann.tomcat.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.catalina.Session;
import org.apache.thrift.TException;

public class CassandraTemplate implements CassandraOperations {

    private static Serializer<String> sessionIdSerializer = new StringSerializer();
    private static Serializer<Long> longSerializer = new LongSerializer();
    private static Serializer<Object> objectSerializer = new ObjectSerializer(CassandraTemplate.class.getClassLoader());

    @Override
    public Object execute(CassandraCallback callback) throws CassandraCallbackException {
        return callback.doInCassandra(getClient());
    }

    protected Client getClient() {
        return null;
    }

    @Override
    public long getCreationTime(String sessionId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setCreationTime(final String sessionId, final long time) {
        execute(new CassandraCallback() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(sessionIdSerializer.toByteBuffer(sessionId), getColumnParent(), getColumnForCreationTime(time), ConsistencyLevel.ONE);
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public long getLastAccessedTime(String sessionId) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setLastAccessedTime(final String sessionId, final long time) {
        execute(new CassandraCallback() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(sessionIdSerializer.toByteBuffer(sessionId), getColumnParent(), getColumnForLastAccessedTime(time), ConsistencyLevel.ONE);
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public Object getAttribute(String sessionId, String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setAttribute(final String sessionId, final String name, final Object value) {
        execute(new CassandraCallback() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(sessionIdSerializer.toByteBuffer(sessionId), getColumnParent(), getColumnForAttribute(name, value), ConsistencyLevel.ONE);
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    // maybe we can use another client
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        });
    }

    @Override
    public void removeAttribute(String sessionId, String name) {
        // TODO Auto-generated method stub
    }

    @Override
    public String[] keys(String sessionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Enumeration<String> getAttributeNames(String sessionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> findSessionKeys() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void addSession(Session session) {
        // TODO Auto-generated method stub
    }

    @Override
    public void removeSession(Session session) {
        // TODO Auto-generated method stub
    }

    // Utilities

    private Column getColumnForCreationTime(long value) {
        return getColumnForName("METADATA-CREATIONTIME", longSerializer.toByteBuffer(value));
    }

    private Column getColumnForLastAccessedTime(long value) {
        return getColumnForName("METADATA-LASTACCESSEDTIME", longSerializer.toByteBuffer(value));
    }

    private Column getColumnForAttribute(String name, Object value) {
        return getColumnForName(name, objectSerializer.toByteBuffer(value));
    }

    private Column getColumnForName(String name, ByteBuffer byteBuffer) {
        return null;
    }

    private ColumnParent getColumnParent() {
        return null;
    }

    protected static ByteBuffer idToByteBuffer(String sessionId) {
        byte[] idBytes = new byte[0];
        try {
            idBytes = sessionId.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return ByteBuffer.wrap(idBytes);
    }

}
