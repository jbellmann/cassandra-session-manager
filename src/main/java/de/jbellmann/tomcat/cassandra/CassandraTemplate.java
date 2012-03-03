package de.jbellmann.tomcat.cassandra;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Serializer;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

public class CassandraTemplate implements CassandraOperations {

    private static final String CF_NAME = "Sessions";

    protected static final String CREATIONTIME_COLUMN_NAME = "METADATA-CREATIONTIME";

    protected static Serializer<String> stringSerializer = new StringSerializer();
    protected static Serializer<Long> longSerializer = new LongSerializer();
    protected static Serializer<Object> objectSerializer = new ObjectSerializer(
            CassandraTemplate.class.getClassLoader());

    private static final BigInteger MAX = new BigInteger("2").pow(127);

    //    @Override
    public <T> T execute(CassandraCallback<T> callback) throws CassandraCallbackException {
        return callback.doInCassandra(getClient());
    }

    protected Client getClient() {
        return null;
    }

    @Override
    public long getCreationTime(final String sessionId) {
        return execute(new CassandraCallback<Long>() {
            @Override
            public Long doInCassandra(Client client) throws RuntimeException {
                try {
                    ColumnOrSuperColumn result = client.get(stringSerializer.toByteBuffer(sessionId),
                            getColumnPath(CREATIONTIME_COLUMN_NAME), ConsistencyLevel.ONE);
                    return longSerializer.fromBytes(result.getColumn().getValue());
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (NotFoundException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void setCreationTime(final String sessionId, final long time) {
        execute(new CassandraCallback<Object>() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(stringSerializer.toByteBuffer(sessionId), getColumnParent(),
                            getColumnForCreationTime(time), ConsistencyLevel.ONE);
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
        execute(new CassandraCallback<Object>() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(stringSerializer.toByteBuffer(sessionId), getColumnParent(),
                            getColumnForLastAccessedTime(time), ConsistencyLevel.ONE);
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
    public Object getAttribute(final String sessionId, final String name) {
        return execute(new CassandraCallback<Object>() {
            @Override
            public Object doInCassandra(Client client) throws RuntimeException {
                try {
                    ColumnOrSuperColumn result = client.get(stringSerializer.toByteBuffer(sessionId),
                            getColumnPath(name), ConsistencyLevel.ONE);
                    return objectSerializer.fromBytes(result.getColumn().getValue());
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (NotFoundException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void setAttribute(final String sessionId, final String name, final Object value) {
        execute(new CassandraCallback<Void>() {
            @Override
            public Void doInCassandra(Client client) throws RuntimeException {
                try {
                    client.insert(stringSerializer.toByteBuffer(sessionId), getColumnParent(),
                            getColumnForAttribute(name, value), ConsistencyLevel.ONE);
                    return null;
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
            }
        });
    }

    @Override
    public void removeAttribute(final String sessionId, final String name) {
        execute(new CassandraCallback<Void>() {
            @Override
            public Void doInCassandra(Client client) throws RuntimeException {
                try {
                    client.remove(stringSerializer.toByteBuffer(sessionId), getColumnPath(name),
                            System.currentTimeMillis(), ConsistencyLevel.ONE);
                    return null;
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
            }
        });
    }

    @Override
    public String[] keys(final String sessionId) {
        return execute(new CassandraCallback<String[]>() {
            @Override
            public String[] doInCassandra(Client client) throws RuntimeException {
                List<String> result = new ArrayList<String>();
                try {
                    List<KeySlice> keysliceList = getClient().get_range_slices(getColumnParent(), getSlicePredicate(),
                            getKeyRange(), ConsistencyLevel.ONE);
                    for (KeySlice keySlice : keysliceList) {
                        if (stringSerializer.fromBytes(keySlice.getKey()).equals(sessionId)) {
                            for (ColumnOrSuperColumn columnOrSuperColumn : keySlice.getColumns()) {
                                if (columnOrSuperColumn.isSetColumn()) {
                                    // it is not a supercolumn
                                    Column column = columnOrSuperColumn.getColumn();
                                    result.add(stringSerializer.fromBytes(column.getName()));
                                }
                            }
                        }
                    }
                    return result.toArray(new String[result.size()]);
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public Enumeration<String> getAttributeNames(String sessionId) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> findSessionKeys() {
        return execute(new CassandraCallback<List<String>>() {
            @Override
            public List<String> doInCassandra(Client client) throws RuntimeException {
                List<String> result = new ArrayList<String>();
                try {
                    List<KeySlice> keysliceList = getClient().get_range_slices(getColumnParent(), getSlicePredicate(),
                            getKeyRange(), ConsistencyLevel.ONE);
                    for (KeySlice keySlice : keysliceList) {
                        result.add(new String(keySlice.getKey()));
                    }
                    return result;
                } catch (InvalidRequestException e) {
                    throw new RuntimeException(e);
                } catch (UnavailableException e) {
                    throw new RuntimeException(e);
                } catch (TimedOutException e) {
                    throw new RuntimeException(e);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void removeSession(final String sessionId) {
        execute(new CassandraCallback<Void>() {
            @Override
            public Void doInCassandra(Client client) throws RuntimeException {
                try {
                    client.remove(stringSerializer.toByteBuffer(sessionId), getEmptyColumnPathForFamily(),
                            System.currentTimeMillis(), ConsistencyLevel.QUORUM);
                    return null;
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
            }
        });
    }

    // Utilities

    protected Column getColumnForCreationTime(long value) {
        return getColumnForName("METADATA-CREATIONTIME", longSerializer.toByteBuffer(value));
    }

    protected Column getColumnForLastAccessedTime(long value) {
        return getColumnForName("METADATA-LASTACCESSEDTIME", longSerializer.toByteBuffer(value));
    }

    protected Column getColumnForAttribute(String name, Object value) {
        return getColumnForName(name, objectSerializer.toByteBuffer(value));
    }

    protected Column getColumnForName(String name, ByteBuffer byteBuffer) {
        Column column = new Column();
        column.setName(stringSerializer.toByteBuffer(name));
        column.setValue(byteBuffer);
        column.setTimestamp(System.currentTimeMillis());
        return column;
    }

    protected SlicePredicate getSlicePredicate() {
        SlicePredicate predicate = new SlicePredicate();
        SliceRange range = new SliceRange();
        range.setStart(new byte[0]);
        range.setFinish(new byte[0]);
        range.setCount(9000);
        predicate.setSlice_range(range);
        return predicate;
    }

    protected KeyRange getKeyRange() {
        KeyRange keyRange = new KeyRange();
        keyRange.setStart_token(BigInteger.ZERO.toString());
        keyRange.setEnd_token(MAX.toString());
        return keyRange;
    }

    protected ColumnParent getColumnParent() {
        ColumnParent columnParent = new ColumnParent();
        columnParent.setColumn_family(CF_NAME);
        return columnParent;
    }

    protected ColumnPath getEmptyColumnPathForFamily() {
        ColumnPath columnPath = new ColumnPath();
        columnPath.setColumn_family(getColumnFamily());
        return columnPath;
    }

    protected String getColumnFamily() {
        return null;
    }

    protected ColumnPath getColumnPath(String columnName) {
        ColumnPath columnPath = new ColumnPath();
        columnPath.setColumn_family("");
        columnPath.setColumn(stringSerializer.toByteBuffer(columnName));
        return columnPath;
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

    static class SingleRowKeyRange {

        public static KeyRange get(String key) {
            KeyRange keyRange = new KeyRange();
            keyRange.setStart_token(key);
            keyRange.setEnd_token(key);
            return keyRange;
        }
    }
}
