package de.jbellmann.tomcat.cassandra;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.List;

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
                    client.insert(idToByteBuffer(sessionId), getColumnParent(), getColumnForCreationTime(time), ConsistencyLevel.ONE);
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
<<<<<<< HEAD
    public void setLastAccessedTime(final String sessionId, final long time) {
        execute(new CassandraCallback() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(idToByteBuffer(sessionId), getColumnParent(), getColumnForLastAccessedTime(time), ConsistencyLevel.ONE);
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
=======
    public void setLastAccessedTime(String sessionId, long time) {
        // TODO Auto-generated method stub
>>>>>>> 50e65500a9bbc7adcf3ff0e196e3eb6ef9dc0628
    }

    @Override
    public Object getAttribute(String sessionId, String name) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
<<<<<<< HEAD
    public void setAttribute(final String sessionId, final String name, final Object value) {
        execute(new CassandraCallback() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(idToByteBuffer(sessionId), getColumnParent(), getColumnForAttribute(name, value), ConsistencyLevel.ONE);
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
=======
    public void setAttribute(String sessionId, String name, Object value) {
        // TODO Auto-generated method stub
>>>>>>> 50e65500a9bbc7adcf3ff0e196e3eb6ef9dc0628
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
<<<<<<< HEAD
        return getColumnForName("METADATA-CREATIONTIME", ByteBuffer.allocate(8).putLong(value));
    }

    private Column getColumnForLastAccessedTime(long value) {
        return getColumnForName("METADATA-LASTACCESSEDTIME", ByteBuffer.allocate(8).putLong(value));
    }

    private Column getColumnForAttribute(String name, Object value) {
        return getColumnForName(name, null);
    }

    private Column getColumnForName(String name, ByteBuffer byteBuffer) {
=======
        return getColumnForName("METADATA-CREATIONTIME", value + "");
    }

    private Column getColumnForName(String name, Object value) {
>>>>>>> 50e65500a9bbc7adcf3ff0e196e3eb6ef9dc0628
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
