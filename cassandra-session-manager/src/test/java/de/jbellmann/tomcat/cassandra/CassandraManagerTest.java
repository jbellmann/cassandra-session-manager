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

import java.io.IOException;

import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Session;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class CassandraManagerTest {

    private final static String TEST_SESSION_ID = "123456789";

    private CassandraTemplate cassandraOperations;
    private Context context;

    //    private Container container;

    @Before
    public void setUp() {
        cassandraOperations = Mockito.mock(CassandraTemplate.class);
        context = Mockito.mock(Context.class);
        Mockito.when(context.getApplicationEventListeners()).thenReturn(new Object[0]);
        //        container = Mockito.mock(Container.class);

    }

    @Test
    public void testSessionCreation() throws LifecycleException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        Assert.assertNotNull(manager.getName());
        Assert.assertEquals(CassandraManager.class.getName() + "/1.0", manager.getInfo());
        Assert.assertEquals("cassandra-manager", manager.getName());
        // create Session
        Session session = manager.createSession(TEST_SESSION_ID);
//        Mockito.verify(cassandraOperations, Mockito.times(1)).setLastAccessedTime(Mockito.eq(TEST_SESSION_ID),
//                Mockito.anyLong());
//        Mockito.verify(cassandraOperations, Mockito.times(1)).setCreationTime(Mockito.eq(TEST_SESSION_ID),
//                Mockito.anyLong());
        Assert.assertNotNull(session);
        Assert.assertNotNull(session.getId());
        Assert.assertNotNull(session.getCreationTime());
        Assert.assertNotNull(session.getLastAccessedTime());
        Mockito.verify(cassandraOperations, Mockito.times(1)).getLastAccessedTime(Mockito.eq(TEST_SESSION_ID));
        Mockito.verify(cassandraOperations, Mockito.times(1)).getCreationTime(Mockito.eq(TEST_SESSION_ID));
    }

    @Test
    public void testFindSession() throws IOException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        Session session = manager.findSession(TEST_SESSION_ID);
        Assert.assertNotNull(session);
        Assert.assertNotNull(session.getId());
        Assert.assertNotNull(session.getCreationTime());
        Assert.assertNotNull(session.getLastAccessedTime());
        Session internal = manager.findSessionInternal(TEST_SESSION_ID);
        Assert.assertNotNull(internal);
        Assert.assertNotNull(internal.getId());
        Assert.assertNotNull(internal.getCreationTime());
        Assert.assertNotNull(internal.getLastAccessedTime());
        Session session2 = manager.findSession(TEST_SESSION_ID);
        Assert.assertNotNull(session2);
        Assert.assertNotNull(session2.getId());
        Assert.assertNotNull(session2.getCreationTime());
        Assert.assertNotNull(session2.getLastAccessedTime());
    }

    @Test
    public void testRemoveSession() throws IOException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        Session session = manager.findSession(TEST_SESSION_ID);
        //
        manager.remove(session);
        Mockito.verify(cassandraOperations, Mockito.times(1)).removeSession(session.getId());
    }

    @Test
    public void testExpireSession() throws IOException {
        CassandraManager manager = new TestCassandraManager(true);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        manager.findSession(TEST_SESSION_ID);
        manager.expireSession(TEST_SESSION_ID);
        //        Mockito.verify(cassandraOperations, Mockito.times(1)).expireSession(Mockito.eq(TEST_SESSION_ID));
    }

    @Test
    public void testGetLastAccessedTime() throws IOException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        //
        Mockito.when(cassandraOperations.getLastAccessedTime(Mockito.anyString())).thenReturn(
                System.currentTimeMillis());
        manager.findSession(TEST_SESSION_ID);
        String lastAccessTime = manager.getLastAccessedTime(TEST_SESSION_ID);
        Assert.assertNotNull(lastAccessTime);
        long lastAccessTimeStamp = manager.getLastAccessedTimestamp(TEST_SESSION_ID);
        Assert.assertNotNull(lastAccessTimeStamp);
        Mockito.verify(cassandraOperations, Mockito.times(3)).getLastAccessedTime(Mockito.eq(TEST_SESSION_ID));
    }

    @Test
    public void testGetCreationTime() throws IOException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        manager.findSession(TEST_SESSION_ID);
        //
        Mockito.when(cassandraOperations.getCreationTime(Mockito.anyString())).thenReturn(System.currentTimeMillis());
        String creationTime = manager.getCreationTime(TEST_SESSION_ID);
        Assert.assertNotNull(creationTime);
        long creationTimeStamp = manager.getCreationTimestamp(TEST_SESSION_ID);
        Assert.assertNotNull(creationTimeStamp);
//        Mockito.verify(cassandraOperations, Mockito.times(2)).getCreationTime(Mockito.eq(TEST_SESSION_ID));
    }

    @Test
    public void testGetSessionAttribute() throws IOException {
        CassandraManager manager = new TestCassandraManager(false);
        manager.setContainer(context);
        manager.setCassandraTemplate(cassandraOperations);
        manager.findSession(TEST_SESSION_ID);
        //
        Mockito.when(cassandraOperations.getAttribute(Mockito.anyString(), Mockito.anyString()))
                .thenReturn("Attribute");
        String attribute = manager.getSessionAttribute(TEST_SESSION_ID, "name");
        Assert.assertNotNull(attribute);
        Assert.assertEquals("Attribute", attribute);
        Mockito.verify(cassandraOperations, Mockito.times(1)).getAttribute(Mockito.eq(TEST_SESSION_ID),
                Mockito.anyString());
    }

    public class TestCassandraManager extends CassandraManager {

        private final boolean wrapSession;

        public TestCassandraManager(boolean wrapSession) {
            this.wrapSession = wrapSession;
            createCache();
        }

        @Override
        protected String generateSessionId() {
            return TEST_SESSION_ID;
        }

        @Override
        public Session findSession(String id) throws IOException {
            CassandraSession session = (CassandraSession) super.findSession(id);
            if (wrapSession) {
                return new SimpleCassandraSession(this);
            }
            return session;
        }

    }
}
