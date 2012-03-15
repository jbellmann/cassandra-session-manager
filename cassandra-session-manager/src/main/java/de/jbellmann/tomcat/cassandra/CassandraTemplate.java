/**
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.jbellmann.tomcat.cassandra;

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

/**
 * Implementation for Cassandra-Data-Access.
 * 
 * @author Joerg Bellmann
 *
 */
public class CassandraTemplate implements CassandraOperations {

    public static final String CREATIONTIME_COLUMN_NAME = "METADATA-CREATIONTIME";
    public static final String LAST_ACCESSTIME_COLUMN_NAME = "METADATA-LASTACCESSTIME";

    private static final List<String> SPECIAL_ATTRIBUTES = new ArrayList<String>();

    static {
        SPECIAL_ATTRIBUTES.add(CREATIONTIME_COLUMN_NAME);
        SPECIAL_ATTRIBUTES.add(LAST_ACCESSTIME_COLUMN_NAME);
    }

    public static final String DEFAULT_CLUSTER_NAME = "TOMCAT_SESSION_MANAGER_CLUSTER";
    public static final String DEFAULT_KEYSPACE_NAME = "TOMCAT_SESSION_MANAGER_KEYSPACE";
    public static final String DEFAULT_COLUMNFAMILY_NAME = "TOMCAT_SESSION_MANAGER_COLUMNFAMILY";

    public static final String DEFAULT_STRATEGY_CLASS_NAME = "org.apache.cassandra.locator.SimpleStrategy";

    public static final int DEFAULT_REPLICATION_FACTOR = 1;

    public static final int DEFAULT_MAX_ACTIVE = 20;
    public static final int DEFAULT_MAX_IDLE = 5;
    public static final int DEFAULT_THRIFT_SOCKET_TIMEOUT = 3000;
    public static final int DEFAULT_MAX_TIME_WHEN_EXHAUSTED = 4000;

    public static final boolean DEFAULT_LOG_SESSIONS_ON_STARTUP = false;

    private final Log log = LogFactory.getLog(CassandraTemplate.class);

    protected Cluster getCluster() {
        return this.cluster;
    }

    protected Keyspace getKeyspace() {
        return this.keyspace;
    }

    //long-live-objects
    private Cluster cluster;
    private Keyspace keyspace;

    //mandantory
    private String clusterName = DEFAULT_CLUSTER_NAME;
    private String keyspaceName = DEFAULT_KEYSPACE_NAME;
    private String columnFamilyName = DEFAULT_COLUMNFAMILY_NAME;
    private String strategyClassName = DEFAULT_STRATEGY_CLASS_NAME;
    private String hosts;
    private int replicationFactor = DEFAULT_REPLICATION_FACTOR;

    //optional
    private int maxActive = DEFAULT_MAX_ACTIVE;
    private int maxIdle = DEFAULT_MAX_IDLE; // not used, method to set not found
    private int thriftSocketTimeout = DEFAULT_THRIFT_SOCKET_TIMEOUT;
    private long maxWaitTimeWhenExhausted = DEFAULT_MAX_TIME_WHEN_EXHAUSTED;

    private boolean logSessionsOnStartup = DEFAULT_LOG_SESSIONS_ON_STARTUP;

    private ObjectSerializer objectSerializer;

    public void initialize(ClassLoader classLoader) {
        log.info("Initialize Cassandra Template ...");
        objectSerializer = new ObjectSerializer(classLoader);
        cluster = HFactory.getOrCreateCluster(getClusterName(), getCassandraHostConfigurator());
        // ColumnFamilyDefinition
        ColumnFamilyDefinition columnFamilyDefinition = HFactory.createColumnFamilyDefinition(getKeyspaceName(),
                getColumnFamilyName(), ComparatorType.BYTESTYPE);
        // KeyspaceDefinition
        KeyspaceDefinition keyspaceDefinition = HFactory.createKeyspaceDefinition(getKeyspaceName(),
                getStrategyClassName(), getReplicationFactor(), Arrays.asList(columnFamilyDefinition));

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
            	if(log.isTraceEnabled()){
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
        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
                StringSerializer.get(), LongSerializer.get());
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(CREATIONTIME_COLUMN_NAME);
        HColumn<String, Long> column = query.execute().get();
        return column != null ? column.getValue() : -1;
    }

    @Override
    public void setCreationTime(final String sessionId, final long time) {
        log.info("Set CREATION_TIME for Session : " + sessionId + " to value " + time);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, this.columnFamilyName,
                HFactory.createColumn(CREATIONTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
    }

    @Override
    public long getLastAccessedTime(final String sessionId) {
		log.info("Get LAST_ACCESSED_TIME for Session : " + sessionId);
        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
                StringSerializer.get(), LongSerializer.get());
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(LAST_ACCESSTIME_COLUMN_NAME);
        HColumn<String, Long> column = query.execute().get();
        return column != null ? column.getValue() : -1;
    }

    @Override
    public void setLastAccessedTime(final String sessionId, final long time) {
        log.info("Set LAST_ACCESSED_TIME for Session : " + sessionId + " to value " + time);
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, this.columnFamilyName,
                HFactory.createColumn(LAST_ACCESSTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
    }

    @Override
    public Object getAttribute(final String sessionId, final String name) {
        log.info("Get attribute '" + name + "' for Session : " + sessionId);
        ColumnQuery<String, String, Object> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
                StringSerializer.get(), objectSerializer);
        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(name);
        QueryResult<HColumn<String, Object>> result = query.execute();
        HColumn<String, Object> column = result.get();
        return column != null ? column.getValue() : null;
    }

    @Override
    public void setAttribute(final String sessionId, final String name, final Object value) {
        log.info("Set attribute '" + name + "' with value " + value.toString() + " for Session : " + sessionId + "");
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.insert(sessionId, this.columnFamilyName,
                HFactory.createColumn(name, value, StringSerializer.get(), objectSerializer));
    }

    @Override
    public void removeAttribute(final String sessionId, final String name) {
        log.info("Remove attribute '" + name + "' for Session : " + sessionId);        
        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
        mutator.delete(sessionId, this.columnFamilyName, name, StringSerializer.get());
    }

    @Override
    public String[] keys(final String sessionId) {
        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(this.keyspace,
                StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        sliceQuery.setColumnFamily(getColumnFamilyName()).setKey(sessionId);
        ColumnSliceIterator<String, String, String> columnSliceIterator = new ColumnSliceIterator<String, String, String>(
                sliceQuery, null, "\uFFFF", false);
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

    //    @Override
    //    public void addSession(final String sessionId) {
    //        long now = System.currentTimeMillis();
    //        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
    //        mutator.addInsertion(sessionId, getColumnFamilyName(),
    //                HFactory.createColumn(CREATIONTIME_COLUMN_NAME, now, StringSerializer.get(), LongSerializer.get()));
    //        mutator.addInsertion(sessionId, getColumnFamilyName(),
    //                HFactory.createColumn(LAST_ACCESSTIME_COLUMN_NAME, now, StringSerializer.get(), LongSerializer.get()));
    //        mutator.execute();
    //    }

    @Override
    public List<String> findSessionKeys() {
        List<String> resultList = new ArrayList<String>();
        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(this.keyspace,
                StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        rangeSlicesQuery.setColumnFamily(this.columnFamilyName);
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
        mutator.delete(sessionId, this.columnFamilyName, null, StringSerializer.get());
    }

    public String getColumnFamilyName() {
        return columnFamilyName;
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.columnFamilyName = columnFamilyName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getKeyspaceName() {
        return keyspaceName;
    }

    public void setKeyspaceName(String keyspaceName) {
        this.keyspaceName = keyspaceName;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getThriftSocketTimeout() {
        return thriftSocketTimeout;
    }

    public void setThriftSocketTimeout(int thriftSocketTimeout) {
        this.thriftSocketTimeout = thriftSocketTimeout;
    }

    public long getMaxWaitTimeWhenExhausted() {
        return maxWaitTimeWhenExhausted;
    }

    public void setMaxWaitTimeWhenExhausted(long maxWaitTimeWhenExhausted) {
        this.maxWaitTimeWhenExhausted = maxWaitTimeWhenExhausted;
    }

    public String getStrategyClassName() {
        return strategyClassName;
    }

    public void setStrategyClassName(String strategyClassName) {
        this.strategyClassName = strategyClassName;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public boolean isLogSessionsOnStartup() {
        return logSessionsOnStartup;
    }

    public void setLogSessionsOnStartup(boolean logSessionsOnStartup) {
        this.logSessionsOnStartup = logSessionsOnStartup;
    }

}
