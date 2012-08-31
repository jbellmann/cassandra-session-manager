package de.jbellmann.tomcat.cassandra.astyanax;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.ObjectSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

import de.jbellmann.tomcat.cassandra.CassandraTemplate;

/**
 * 
 * 
 * 
 * @author Joerg Bellmann
 * 
 */
public class AstyanaxCassandraTemplate extends CassandraTemplate {
    
    private final Log log = LogFactory.getLog(CassandraTemplate.class);
    
    AstyanaxContext<Cluster> context;
    private Keyspace keyspace;
    private ColumnFamily<String, Object> columnFamily;

    @Override
    public void initialize(ClassLoader classLoader) {
        
        context = new AstyanaxContext.Builder()
                .forCluster(getClusterName())
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
                .withConnectionPoolConfiguration(
                        new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setPort(9160)
                        .setMaxConnsPerHost(1)
                        .setSeeds("127.0.0.1:9160")
                        .setMaxTimeoutCount(5)
                        .setConnectTimeout(10000))
                .withConnectionPoolMonitor(new Slf4jConnectionPoolMonitorImpl())
                .buildCluster(ThriftFamilyFactory.getInstance());
        
        context.start();
        
        
        try{
            if(context.getEntity().describeKeyspace(getKeyspaceName()) == null){
                KeyspaceDefinition ksDef = context.getEntity().makeKeyspaceDefinition();
                
                Map<String, String> stratOptions = new HashMap<String, String>();
                stratOptions.put("replication_factor", "1");
                
                ksDef.setName(getKeyspaceName())
                        .setStrategyOptions(stratOptions)
                        .setStrategyClass(getStrategyClassName())
                        .addColumnFamily(
                                context.getEntity().makeColumnFamilyDefinition()
                                        .setName(getColumnFamilyName())
                                        .setComparatorType("BytesType")
                                        .setKeyValidationClass("BytesType"));
                
                context.getEntity().addKeyspace(ksDef);
            }
        }catch(Exception e){
            log.error(e.getMessage(), e);
        }
        keyspace = context.getEntity().getKeyspace(getKeyspaceName());
        // in hector we set the classloader to the objectSerializer
        columnFamily = new ColumnFamily<String, Object>(getColumnFamilyName(), StringSerializer.get(), ObjectSerializer.get());
    }

    @Override
    public void shutdown() {
        log.info("Shuttingdown context ...");
        context.shutdown();
        log.info("Context is down");
    }
    
    @Override
    public long getCreationTime(final String sessionId) {
        log.info("Get CREATION_TIME for Session : " + sessionId);
//        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
//                StringSerializer.get(), LongSerializer.get());
//        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(CREATIONTIME_COLUMN_NAME);
//        HColumn<String, Long> column = query.execute().get();
//        return column != null ? column.getValue() : -1;
        try {
            OperationResult<ColumnList<Object>> result = keyspace.prepareQuery(columnFamily).getKey(sessionId).execute();
            Column<Object> column = result.getResult().getColumnByName(CREATIONTIME_COLUMN_NAME);
            return column != null ? column.getLongValue() : -1;
        } catch (ConnectionException e) {
            log.error("Could not get 'creationTime' because of 'ConnectionException'", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setCreationTime(final String sessionId, final long time) {
        log.info("Set CREATION_TIME for Session : " + sessionId + " to value " + time);
//        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
//        mutator.insert(sessionId, this.columnFamilyName,
//                HFactory.createColumn(CREATIONTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.withRow(columnFamily, sessionId).putColumn(CREATIONTIME_COLUMN_NAME, time, null);
        try{
            mutation.execute();
        }catch(ConnectionException e){
            log.error("Could not insert 'creationTime' for sessionId " + sessionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getLastAccessedTime(final String sessionId) {
        log.info("Get LAST_ACCESSED_TIME for Session : " + sessionId);
//        ColumnQuery<String, String, Long> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
//                StringSerializer.get(), LongSerializer.get());
//        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(LAST_ACCESSTIME_COLUMN_NAME);
//        HColumn<String, Long> column = query.execute().get();
//        return column != null ? column.getValue() : -1;
        try {
            OperationResult<ColumnList<Object>> result = keyspace.prepareQuery(columnFamily).getKey(sessionId).execute();
//            Column<Object> column = keyspace.prepareQuery(columnFamily).getKey(sessionId).getColumn(LAST_ACCESSTIME_COLUMN_NAME).execute().getResult();
            Column<Object> column = result.getResult().getColumnByName(LAST_ACCESSTIME_COLUMN_NAME);
            return column != null ? column.getLongValue() : -1;
        } catch (ConnectionException e) {
            log.error("Could not get 'creationTime' because of 'ConnectionException'", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setLastAccessedTime(final String sessionId, final long time) {
        log.info("Set LAST_ACCESSED_TIME for Session : " + sessionId + " to value " + time);
//        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
//        mutator.insert(sessionId, this.columnFamilyName,
//                HFactory.createColumn(LAST_ACCESSTIME_COLUMN_NAME, time, StringSerializer.get(), LongSerializer.get()));
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.withRow(columnFamily, sessionId).putColumn(LAST_ACCESSTIME_COLUMN_NAME, time, null);
        try{
            mutation.execute();
        }catch(ConnectionException e){
            log.error("Could not insert 'lastAccessTime' for sessionId " + sessionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object getAttribute(final String sessionId, final String name) {
        log.info("Get attribute '" + name + "' for Session : " + sessionId);
//        ColumnQuery<String, String, Object> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(),
//                StringSerializer.get(), objectSerializer);
//        query.setColumnFamily(getColumnFamilyName()).setKey(sessionId).setName(name);
//        QueryResult<HColumn<String, Object>> result = query.execute();
//        HColumn<String, Object> column = result.get();
//        return column != null ? column.getValue() : null;
        try {
            OperationResult<ColumnList<Object>> result = keyspace.prepareQuery(columnFamily).getKey(sessionId).execute();
            Column<Object> column = result.getResult().getColumnByName(name);
            return column != null ? column.getValue(ObjectSerializer.get()) : null;
        } catch (ConnectionException e) {
            log.error("Could not get 'creationTime' because of 'ConnectionException'", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setAttribute(final String sessionId, final String name, final Object value) {
        log.info("Set attribute '" + name + "' with value " + value.toString() + " for Session : " + sessionId + "");
//        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
//        mutator.insert(sessionId, this.columnFamilyName,
//                HFactory.createColumn(name, value, StringSerializer.get(), objectSerializer));
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.withRow(columnFamily, sessionId).putColumn(name, value, ObjectSerializer.get(), null);
        try{
            mutation.execute();
        }catch(ConnectionException e){
            log.error("Could not 'setAttribute' for column with name: '"+ name+ "' for sessionId " + sessionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void removeAttribute(final String sessionId, final String name) {
        log.info("Remove attribute '" + name + "' for Session : " + sessionId);        
//        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
//        mutator.delete(sessionId, this.columnFamilyName, name, StringSerializer.get());
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.withRow(columnFamily, sessionId).deleteColumn(name);
        try{
            mutation.execute();
        }catch(ConnectionException e){
            log.error("Could not delete column '" + name + "' for sessionId " + sessionId, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String[] keys(final String sessionId) {
//        SliceQuery<String, String, String> sliceQuery = HFactory.createSliceQuery(this.keyspace,
//                StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
//        sliceQuery.setColumnFamily(getColumnFamilyName()).setKey(sessionId);
//        ColumnSliceIterator<String, String, String> columnSliceIterator = new ColumnSliceIterator<String, String, String>(
//                sliceQuery, null, "\uFFFF", false);
        List<String> resultList = new ArrayList<String>();
//        //
//        while (columnSliceIterator.hasNext()) {
//            String columnName = columnSliceIterator.next().getName();
//            if (!(SPECIAL_ATTRIBUTES.contains(columnName))) {
//                resultList.add(columnName);
//            }
//        }
        ColumnList<Object> columnList = null;
        try {
            columnList = keyspace.prepareQuery(columnFamily).getKey(sessionId).execute().getResult();
        } catch (ConnectionException e) {
            log.error("Could not get all column names for session " + sessionId);
            throw new RuntimeException(e);
        }
        for(Column<Object> column : columnList){
            resultList.add(column.getName().toString());
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
//        RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory.createRangeSlicesQuery(this.keyspace,
//                StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
//        rangeSlicesQuery.setColumnFamily(this.columnFamilyName);
//        rangeSlicesQuery.setKeys("", "");
//        rangeSlicesQuery.setReturnKeysOnly();
//
//        //        rangeSlicesQuery.setRowCount(500);
//
//        QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
//        for (Row<String, String, String> row : result.get().getList()) {
//            resultList.add(row.getKey());
//        }
        Rows<String, Object> result;
        try {
            result =
                    keyspace.prepareQuery(columnFamily)
                      .getAllRows()
                      .withColumnRange(new RangeBuilder().setLimit(0).build())  // RangeBuilder will be available in version 1.13
                      .execute().getResult();
        } catch (ConnectionException e) {
            log.error("Could not get the keys for all rows", e);
            throw new RuntimeException(e);
        }
        for(Row<String,Object> row : result){
            resultList.add(row.getKey());
        }
        return resultList;
    }

    @Override
    public void removeSession(final String sessionId) {
//        Mutator<String> mutator = HFactory.createMutator(this.keyspace, StringSerializer.get());
//        mutator.delete(sessionId, this.columnFamilyName, null, StringSerializer.get());
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.withRow(columnFamily, sessionId).delete();
        try{
            mutation.execute();
        }catch(ConnectionException e){
            log.error("Could not delete row for sessionId '" + sessionId, e);
            throw new RuntimeException(e);
        }
    }

}
