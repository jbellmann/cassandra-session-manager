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
package de.jbellmann.tomcat.cassandra.hector;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Context;
import org.apache.catalina.connector.Request;
import org.apache.thrift.transport.TTransportException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import de.jbellmann.tomcat.cassandra.CassandraManager;
import de.jbellmann.tomcat.cassandra.CassandraTemplate;

/**
 * 
 * @author Joerg Bellmann
 *
 */
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
        template.initialize(getClass().getClassLoader());
        // testnonex

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

    @Test
    public void testSessionNotExists() {
        CassandraTemplate template = new TestCassandraTemplate();
        template.setLogSessionsOnStartup(true);
        template.initialize(getClass().getClassLoader());
        String testSessionId = UUID.randomUUID().toString().replace("-", "");
        long lasAccessedTime = template.getLastAccessedTime(testSessionId);
        long testcreationTime = template.getCreationTime(testSessionId);

        Assert.assertEquals("Should be -1 because it does not exist in db", -1, lasAccessedTime);
        Assert.assertEquals("Should be -1 because it does not exist in db", -1, testcreationTime);
    }

    public void testGetSession() {
        String requestedSessionId = "REQUESTED_SESSION_ID";

        Context context = Mockito.mock(Context.class);

        CassandraTemplate template = new TestCassandraTemplate();
        template.setLogSessionsOnStartup(true);
        template.initialize(getClass().getClassLoader());

        CassandraManager manager = new HectorCassandraManager();
        manager.setCassandraTemplate(template);
        Mockito.when(context.getManager()).thenReturn(manager);
        Request request = new Request();
        request.setContext(context);
        // fake cookie sent a value
        request.setRequestedSessionId(requestedSessionId);
        HttpSession session = request.getSession();
        Assert.assertFalse(requestedSessionId.equals(session.getId()));
    }

    protected String createRandomSessionId() {
        return UUID.randomUUID().toString().replaceAll("-", "");
    }

    class TestCassandraTemplate extends HectorCassandraTemplate {

        public TestCassandraTemplate() {
            super();
            super.setHosts("localhost:9160");
            super.setMaxActive(5);
        }

    }

}
