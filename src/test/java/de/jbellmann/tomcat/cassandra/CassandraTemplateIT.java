package de.jbellmann.tomcat.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CassandraTemplateIT {

    @Before
    public void setUp() throws TTransportException {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testSetCreationTime() throws InterruptedException {
        CassandraTemplate template = new TestCassandraTemplate();
        template.initialize();
        for (int i = 0; i < 20; i++) {
            template.setCreationTime(createRandomSessionId(), System.currentTimeMillis());
        }
        List<String> sessionIdList = template.findSessionKeys();
        System.out.println("Found sessionIds : " + sessionIdList.size());
        Assert.assertEquals("Should be 20 Keys", 20, sessionIdList.size());

        for (String sessionId : sessionIdList) {
            long creationTime = template.getCreationTime(sessionId);
            System.out.println("Session : " + sessionId + " -- creationTime : " + creationTime);
        }
        System.out.println("REMOVING 5 ROWS");
        // Remove five sessions
        List<String> sublist = sessionIdList.subList(0, 4);
        for (String sessionId : sublist) {
            template.removeSession(sessionId);
        }
        sessionIdList = template.findSessionKeys();
        System.out.println("Found sessionIds : " + sessionIdList.size());
        // http://wiki.apache.org/cassandra/DistributedDeletes
        // not possible to see result
        //Assert.assertEquals("Should only 15 Keys", 15, sessionIdList.size());
        System.out.println("Columns for deleted Rows:");
        for (String sessionId : sublist) {
            String[] keys = template.keys(sessionId);
            //            Assert.assertEquals("Should not have a column", 0, keys.length);
            System.out.println("For session " + sessionId + " found keys: " + Arrays.asList(keys).toString());
        }
        System.out.println("Columns for existent Rows: ");
        for (String sessionId : sessionIdList.subList(5, 9)) {
            String[] keys = template.keys(sessionId);
            //            Assert.assertTrue("Should have a column", keys.length > 0);
            System.out.println("For session " + sessionId + " found keys: " + Arrays.asList(keys).toString());
        }
    }

    protected String createRandomSessionId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    class TestCassandraTemplate extends CassandraTemplate {

        public TestCassandraTemplate() {
            super();
            super.setHosts("localhost:9160");
            super.setMaxActive(5);
        }

    }

}
