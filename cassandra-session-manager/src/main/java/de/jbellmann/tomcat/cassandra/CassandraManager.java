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
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.Container;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleState;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class CassandraManager extends ManagerBase {

    private static final String INFO = CassandraManager.class.getName() + "/1.0";

    private final Log log = LogFactory.getLog(CassandraManager.class);

    protected String name = "cassandra-manager";
    protected AtomicInteger rejected = new AtomicInteger(0);

    protected CassandraTemplate cassandraTemplate = new CassandraTemplate();

    void setName(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getInfo() {
        return INFO;
    }

    protected CassandraOperations getCassandraOperations() {
        return this.cassandraTemplate;
    }

    protected void setCassandraTemplate(CassandraTemplate cassandraTemplate) {
        this.cassandraTemplate = cassandraTemplate;
    }

    public ClassLoader getClassLoader() {
        ClassLoader clazzLoader = getClass().getClassLoader();
        Container container = getContainer();
        if (container != null) {
            Loader loader = container.getLoader();
            if (loader != null) {
                ClassLoader classLoader = loader.getClassLoader();
                if (classLoader != null) {
                    clazzLoader = classLoader;
                }
            }
        }
        return clazzLoader;
    }

    @Override
    public int getRejectedSessions() {
        return rejected.get();
    }

    public void setRejectedSessions(int i) {
        rejected.set(i);
    }

    /**
     * Should we really load all sessions. 10000, 1000000, 10000000000. Should we?
     * 
     */
    @Override
    public void load() {
        List<String> sessionIds = getCassandraOperations().findSessionKeys();
        for (String sessionId : sessionIds) {
            try {
                if (log.isDebugEnabled()) {
                    log.debug("Loading session : " + sessionId);
                }
                findSession(sessionId);
            } catch (IOException e) {
                log.error("IOException on CassandraManager.load() with sessionId: " + sessionId, e);
            }
        }
    }

    @Override
    protected void startInternal() throws LifecycleException {
        log.info("Starting Cassandra Session Manager");
        try {
            this.cassandraTemplate.initialize(getClassLoader());
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            log.error(sm.getString("standardManager.managerLoad"), t);
        }
        super.startInternal();
        // Load unloaded sessions, if any
        try {
            load();
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            log.error(sm.getString("standardManager.managerLoad"), t);
        }

        setState(LifecycleState.STARTING);
        log.info("Cassandra Session Manager started");
    }

    @Override
    protected void stopInternal() throws LifecycleException {
        log.info("Stopping Cassandra Session Manager");
        try {
            this.cassandraTemplate.shutdown();
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            log.error(sm.getString("standardManager.managerUnload"), t);
        }
        setState(LifecycleState.STOPPING);

        // Write out sessions
        try {
            unload();
        } catch (Throwable t) {
            ExceptionUtils.handleThrowable(t);
            log.error(sm.getString("standardManager.managerUnload"), t);
        }

        // Expire all active sessions
        Session[] sessions = findSessions();
        for (int i = 0; i < sessions.length; i++) {
            Session session = sessions[i];
            try {
                if (session.isValid()) {
                    session.expire();
                }
            } catch (Throwable t) {
                ExceptionUtils.handleThrowable(t);
            } finally {
                // Measure against memory leaking if references to the session
                // object are kept in a shared field somewhere
                session.recycle();
            }
        }
        log.info("Cassandra Session Manager stopped");
        // Require a new random number generator if we are restarted
        super.stopInternal();
    }

    @Override
    public void unload() {
        // NO-OP
    }

    @Override
    public Session findSession(String id) throws IOException {
        Session sess = super.findSession(id);
        if (null != sess) {
            if (log.isDebugEnabled()) {
                log.debug("Returning cached session: " + sess);
            }
            return sess;
        }
        long lastAccessedTime = getCassandraOperations().getLastAccessedTime(id);
        if (lastAccessedTime < 0) {
            // no session found in cassandra
            return null;
        } else {
            sess = createSession(id);
            sessions.put(sess.getId(), sess);
        }
        return sess;
    }

    protected Session findSessionInternal(String id) {
        try {
            return super.findSession(id);
        } catch (IOException e) {
            log.error("IOException on Cassandra.findSessionInternal() with sessionId " + id, e);
        }
        return null;
    }

    @Override
    public Session[] findSessions() {
        List<Session> sessions = new ArrayList<Session>();
        List<String> sessionIds = getCassandraOperations().findSessionKeys();
        for (String sessionId : sessionIds) {
            Session s = null;
            try {
                s = findSession(sessionId);
            } catch (IOException e) {
                log.error("IOException when finding session with id " + sessionId, e);
            }
            if (s != null) {
                sessions.add(s);
            }
        }
        return sessions.toArray(new Session[sessions.size()]);
    }

    @Override
    public void remove(Session session) {
        getCassandraOperations().removeSession(session.getId());
    }

    @Override
    protected StandardSession getNewSession() {
        return new CassandraSession(this);
    }

    @Override
    public HashMap<String, String> getSession(String sessionId) {
        return super.getSession(sessionId);
    }

    @Override
    public void expireSession(String sessionId) {
        Session session = null;
        try {
            session = findSession(sessionId);
        } catch (IOException e) {
            log.error("IOException on CassandraManager.expireSession()-method with sessionId:" + sessionId, e);
        }
        if (session != null) {
            session.expire();
        }
    }

    @Override
    public String getCreationTime(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? new Date(session.getCreationTime()).toString() : "";
    }

    @Override
    public long getCreationTimestamp(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? session.getCreationTime() : -1;
    }

    @Override
    public long getLastAccessedTimestamp(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? session.getLastAccessedTime() : -1;
    }

    @Override
    public String getLastAccessedTime(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? new Date(session.getLastAccessedTime()).toString() : "";
    }

    @Override
    public String getSessionAttribute(String sessionId, String key) {
        return getCassandraOperations().getAttribute(sessionId, key).toString();
    }

    @Override
    public Session createEmptySession() {
        return (getNewSession());
    }

    @Override
    public Session createSession(String sessionId) {
        CassandraSession session = (CassandraSession) createEmptySession();
        session.setNew(true);
        session.setValid(true);
        session.setMaxInactiveInterval(maxInactiveInterval);
        String id = sessionId;
        if (id == null) {
            id = generateSessionId();
        }
        session.setId(id, true);
        session.setCreationTime(System.currentTimeMillis());
        session.setLastAccessedTime(System.currentTimeMillis());
        sessionCounter++;
        //
        return session;
    }

    // GETTER-SETTER

    public String getColumnFamilyName() {
        return this.cassandraTemplate.getColumnFamilyName();
    }

    public void setColumnFamilyName(String columnFamilyName) {
        this.cassandraTemplate.setColumnFamilyName(columnFamilyName);
    }

    public String getClusterName() {
        return this.cassandraTemplate.getClusterName();
    }

    public void setClusterName(String clusterName) {
        this.cassandraTemplate.setClusterName(clusterName);
    }

    public String getKeyspaceName() {
        return this.cassandraTemplate.getKeyspaceName();
    }

    public void setKeyspaceName(String keyspaceName) {
        this.cassandraTemplate.setKeyspaceName(keyspaceName);
    }

    public String getHosts() {
        return this.cassandraTemplate.getHosts();
    }

    public void setHosts(String hosts) {
        this.cassandraTemplate.setHosts(hosts);
    }

    public int getMaxActiveConnections() {
        return this.cassandraTemplate.getMaxActive();
    }

    public void setMaxActiveConnections(int maxActive) {
        this.cassandraTemplate.setMaxActive(maxActive);
    }

    public int getMaxIdle() {
        return this.cassandraTemplate.getMaxIdle();
    }

    public void setMaxIdle(int maxIdle) {
        this.cassandraTemplate.setMaxIdle(maxIdle);
    }

    public int getThriftSocketTimeout() {
        return this.cassandraTemplate.getThriftSocketTimeout();
    }

    public void setThriftSocketTimeout(int thriftSocketTimeout) {
        this.cassandraTemplate.setThriftSocketTimeout(thriftSocketTimeout);
    }

    public long getMaxWaitTimeWhenExhausted() {
        return this.cassandraTemplate.getMaxWaitTimeWhenExhausted();
    }

    public void setMaxWaitTimeWhenExhausted(long maxWaitTimeWhenExhausted) {
        this.cassandraTemplate.setMaxWaitTimeWhenExhausted(maxWaitTimeWhenExhausted);
    }

    public String getStrategyClassName() {
        return this.cassandraTemplate.getStrategyClassName();
    }

    public void setStrategyClassName(String strategyClassName) {
        this.cassandraTemplate.setStrategyClassName(strategyClassName);
    }

    public int getReplicationFactor() {
        return this.cassandraTemplate.getReplicationFactor();
    }

    public void setReplicationFactor(int replicationFactor) {
        this.cassandraTemplate.setReplicationFactor(replicationFactor);
    }

    public boolean isLogSessionsOnStartup() {
        return this.cassandraTemplate.isLogSessionsOnStartup();
    }

    public void setLogSessionsOnStartup(boolean logSessionsOnStartup) {
        this.cassandraTemplate.setLogSessionsOnStartup(logSessionsOnStartup);
    }

}
