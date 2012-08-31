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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import javax.servlet.ServletContext;
import javax.servlet.SessionCookieConfig;
import javax.servlet.SessionTrackingMode;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.catalina.Container;
import org.apache.catalina.Context;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
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
public class RequestTest {

    private static final String REQUESTED_SESSION_ID = "REQUESTED_SESSION_ID";

    private Context context;
    private Response response;

    @Before
    public void setUp() throws LifecycleException {
        Container container = Mockito.mock(Container.class);
        Mockito.when(container.getName()).thenReturn("TEST_CONTAINER");
        context = Mockito.mock(Context.class);
        CassandraManager manager = new MockCassandraManager();
        Mockito.when(context.getManager()).thenReturn(manager);
        Mockito.when(context.getName()).thenReturn("TEST_CONTEXT");
        Mockito.when(context.getParent()).thenReturn(container);
        // CookieConfig
        SessionCookieConfig cookieConfig = Mockito.mock(SessionCookieConfig.class);
        Mockito.when(cookieConfig.getName()).thenReturn("JSESSIONID");
        Mockito.when(cookieConfig.getComment()).thenReturn("TEST-COOKIE-COMMENT");
        Mockito.when(cookieConfig.getMaxAge()).thenReturn(-1);
        Mockito.when(cookieConfig.getPath()).thenReturn("/");
        //
        ServletContext servletContext = Mockito.mock(ServletContext.class);
        Mockito.when(servletContext.getEffectiveSessionTrackingModes()).thenReturn(getTrackingModes());
        Mockito.when(servletContext.getSessionCookieConfig()).thenReturn(cookieConfig);
        //
        Mockito.when(context.getServletContext()).thenReturn(servletContext);
        Mockito.when(context.getSessionCookieName()).thenReturn("JSESSIONID");

        //
        response = Mockito.mock(Response.class);
        HttpServletResponse servletResponse = Mockito.mock(HttpServletResponse.class);
        Mockito.when(servletResponse.isCommitted()).thenReturn(false);
        Mockito.when(response.getResponse()).thenReturn(servletResponse);
        CassandraTemplate template = new MockCassandraTemplate();
        template.initialize(getClass().getClassLoader());
        manager.setContainer(context);
        manager.setCassandraTemplate(template);
        manager.setMaxInactiveInterval(1000 * 60 * 30);
        manager.start();
    }

    @Test
    public void testGetSession() throws LifecycleException {
        Request request = new Request();
        request.setContext(context);
        request.setResponse(response);
        // fake cookie sent a value
        request.setRequestedSessionId(REQUESTED_SESSION_ID);

        HttpSession session = request.getSession();
        Assert.assertFalse(REQUESTED_SESSION_ID.equals(session.getId()));
    }

    @Test
    public void testRecycleSessionId() {
        Mockito.when(context.getSessionCookiePath()).thenReturn("/");
        Request request = new Request();
        request.setContext(context);
        request.setResponse(response);
        // fake cookie sent a value
        request.setRequestedSessionId(REQUESTED_SESSION_ID);
        request.setRequestedSessionCookie(true);

        HttpSession session = request.getSession();
        Assert.assertEquals(REQUESTED_SESSION_ID, session.getId());
    }

    private Set<SessionTrackingMode> getTrackingModes() {
        Set<SessionTrackingMode> trackingModes = new HashSet<SessionTrackingMode>();
        trackingModes.add(SessionTrackingMode.COOKIE);
        return trackingModes;
    }

    class MockCassandraTemplate extends HectorCassandraTemplate {

        @Override
        public void initialize(ClassLoader classLoader) {
            // do nothing
        }

        @Override
        public long getLastAccessedTime(String sessionId) {
            return sessionId.equals(REQUESTED_SESSION_ID) ? -1 : System.currentTimeMillis();
        }

        //        @Override
        //        public void addSession(String sessionId) {
        //        }

        @Override
        public void setCreationTime(String sessionId, long time) {
        }

        @Override
        public void setLastAccessedTime(String sessionId, long time) {
        }

    }

    class MockCassandraManager extends CassandraManager {

        @Override
        protected String generateSessionId() {
            return UUID.randomUUID().toString().replace("-", "");
        }

        @Override
        public void load() {
        }

        @Override
        public CassandraTemplate createCassandraTemplate() {
            return new MockCassandraTemplate();
        }

    }

}
