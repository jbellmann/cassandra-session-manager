package de.jbellmann.tomcat.cassandra.hector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.ObjectSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import de.jbellmann.tomcat.cassandra.CassandraTemplate;

/**
 * Implements an {@link CassandraTemplate} with the Hector-Cassandra-Client.
 * 
 * 
 * @author Joerg Bellmann
 *
 */
public class HectorCassandraTemplate extends CassandraTemplate {

    private final Log log = LogFactory.getLog(HectorCassandraTemplate.class);

    //long-live-objects
    private Cluster cluster;
    private Keyspace keyspace;

    protected Cluster getCluster() {
        return this.cluster;
    }

    protected Keyspace getKeyspace() {
        return this.keyspace;
    }

    private ObjectSerializer objectSerializer;

    public void initialize(ClassLoader classLoader) {
        log.info("Initialize Cassandra Template ...");
        objectSerializer = new ObjectSerializer(classLoader);
        cluster = HFactory.getOrCreateCluster(getClusterName(), getCassandraHostConfigurator());
        // ColumnFamilyDefinition
        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(getKeyspaceName(), getColumnFamilyName(),
                ComparatorType.BYTESTYPE);
        // KeyspaceDefinition
        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(getKeyspaceName(), getStrategyClassName(), getReplicationFactor(),
                Arrays.asList(columnFamilyDefinition));

        KeyspaceDefinition description = cluster.describeKeyspace(getKeyspaceName());
        if (description == null) {
            // keyspace to cluster
            cluster.addKeyspace(keyspaceDefinition, true);
        }

        keyspace = HFactory.createKeyspace(getKeyspaceName(), cluster);
        log.info("Cassandra-Template initialized");
        if (isLogSessionsOnStartup()) {
            List<String> sessionIdList = findSessionKeys();
            log.info("Found " + sessionIdList.size() + " Sessions in DB");
            for (final String sessionId : sessionIdList) {
                if (log.isTraceEnabled()) {
                    log.info("SessionId found : " + sessionId);
                }
            }
        }
    }

    public void shutdown() {
        log.info("Release Connections ...");
        this.cluster.getConnectionManager().shutdown();
        log.info("Connections released");
    }

    protected CassandraHostConfigurator getCassandraHostConfigurator() {
        CassandraHostConfigurator configurator = new CassandraHostConfigurator(getHosts());
        configurator.setMaxActive(getMaxActive());
        configurator.setMaxWaitTimeWhenExhausted(getMaxWaitTimeWhenExhausted());
        configurator.setCassandraThriftSocketTimeout(getThriftSocketTimeout());
        return configurator;
    }

    @Override
    public long getCreationTime(final String sessionId) {
        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(CREATIONTIME_COLUMN_NAME);
        HColumn<String, Long> column = query.execute().get();
        return column != null ? column.getValue() : -1;
    }

    @Override
    public void setCreationTime(final String sessionId, final long time) {
        log.info("Set CREATION_TIME for Session : " + sessionId + " to value " + time);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, getColumnFamilyName(), HFactory.createColumn(CREATIONTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
    }

    @Override
    public long getLastAccessedTime(final String sessionId) {
        log.info("Get LAST_ACCESSED_TIME for Session : " + sessionId);
        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(LAST_ACCESSTIME_COLUMN_NAME);
        HColumn<String, Long> column = query.execute().get();
        return column != null ? column.getValue() : -1;
    }

    @Override
    public void setLastAccessedTime(final String sessionId, final long time) {
        log.info("Set LAST_ACCESSED_TIME for Session : " + sessionId + " to value " + time);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, getColumnFamilyName(), HFactory.createColumn(LAST_ACCESSTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
    }

    @Override
    public Object getAttribute(final String sessionId, final String name) {
        log.info("Get attribute '" + name + "' for Session : " + sessionId);
        ColumnQuery<String, String, Object> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(), StringSerializer.get(), objectSerializer);
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(name);
        QueryResult<HColumn<String, Object>> result = query.execute();
        HColumn<String, Object> column = result.get();
        return column != null ? column.getValue() : null;
    }

    @Override
    public void setAttribute(final String sessionId, final String name, final Object value) {
        log.info("Set attribute '" + name + "' with value " + value.toString() + " for Session : " + sessionId + "");
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, getColumnFamilyName(), HFactory.createColumn(name, value, StringSerializer.get(), objectSerializer));
    }

    @Override
    public void removeAttribute(final String sessionId, final String name) {
        log.info("Remove attribute '" + name + "' for Session : " + sessionId);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.delete(sessionId, getColumnFamilyName(), name, StringSerializer.get());
    }

    @Override
    public String[] keys(final String sessionId) {
        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(this.keyspace, StringSerializer.get(), StringSerializer.get(),
                StringSerializer.get());
        sliceQuery.setColumnFamily(getColumnFamilyName()).setKey(sessionId);
        ColumnSliceIterator<String, String, String> columnSliceIterator = new ColumnSliceIterator<String, String, String>(sliceQuery, null, "\uFFFF", false);
        List<String> resultList = new ArrayList<String>();
        //
        while (columnSliceIterator.hasNext()) {
            String columnName = columnSliceIterator.next().getName();
            if (!(SPECIAL_ATTRIBUTES.contains(columnName))) {
                resultList.add(columnName);
            }
        }
        return resultList.toArray(new String[resultList.size()]);
    }

    @Override
    public Enumeration<String> getAttributeNames(String sessionId) {
        return Collections.enumeration(Arrays.asList(keys(sessionId)));
    }

    @Override
    public List<String> findSessionKeys() {
        List<String> resultList = new ArrayList<String>();
        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(this.keyspace, StringSerializer.get(),
                StringSerializer.get(), StringSerializer.get());
        rangeSlicesQuery.setColumnFamily(getColumnFamilyName());
        rangeSlicesQuery.setKeys("", "");
        rangeSlicesQuery.setReturnKeysOnly();

        //        rangeSlicesQuery.setRowCount(500);

        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
        for (Row<String, String, String> row : result.get().getList()) {
            resultList.add(row.getKey());
        }
        return resultList;
    }

    @Override
    public void removeSession(final String sessionId) {
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.delete(sessionId, getColumnFamilyName(), null, StringSerializer.get());
    }

}
