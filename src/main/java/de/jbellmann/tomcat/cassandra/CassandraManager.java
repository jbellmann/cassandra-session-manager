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
import org.apache.commons.lang.StringUtils;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;
import org.apache.tomcat.util.ExceptionUtils;

/**
 * 
 * @author jbellmann
 *
 */
public class CassandraManager extends ManagerBase {

    private static final String info = CassandraManager.class.getName() + "/1.0";

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
        return info;
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

    @Override
    public void load() {
        List<String> sessionIds = getCassandraOperations().findSessionKeys();
        for (String sessionId : sessionIds) {
            try {
                findSession(sessionId);
            } catch (IOException e) {
                log.error("IOException on CassandraManager.load() with sessionId: " + sessionId, e);
            }
        }
        //            riak.getBucketSchema(name, true)?.keys?.each {
        //              if (log.isDebugEnabled()) {
        //                log.debug("Loading session $it");
        //              }
        //              findSession(it);
        //            }
    }

    @Override
    protected void startInternal() throws LifecycleException {
        log.info("Starting Cassandra Session Manager");
        try {
            this.cassandraTemplate.initialize();
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
        Session sessions[] = findSessions();
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
    public void add(Session session) {
        getCassandraOperations().addSession(session.getId());
        //            def metadata = [
        //                creationTime: System.currentTimeMillis(),
        //                lastAccessedTime: System.currentTimeMillis()
        //            ]
        //            if (log.debugEnabled) {
        //              log.debug "Saving session ${session.id}"
        //            }
        //            riak.set(name, session.id, metadata)
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

        sess = createSession(id);
        ((CassandraSession) sess).setIdInternal(id);
        sessions.put(id, sess);
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
        //            def s = []
        //            riak.getBucketSchema(name, true)?.keys?.each {
        //              s << findSession(it)
        //            }
        //            return s.toArray(new Session[s.size()])
    }

    @Override
    public void remove(Session session) {
        getCassandraOperations().removeSession(session.getId());
        //        riak.delete(name, session.id);
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
        //            findSession(sessionId)?.expire()
    }

    @Override
    public String getCreationTime(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? new Date(session.getCreationTime()).toString() : "";
        //        try {
        //            session = findSession(sessionId);
        //        } catch (IOException e) {
        //            log.error("IOException on CassandraManager.getCreationTime()-method with sessionId " + sessionId, e);
        //        }
        //        if (session != null) {
        //            return new Date(session.getCreationTime()).toString();
        //        }
        //        return "";
        //            return findSession(sessionId)?.getCreationTime()
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
        //            return findSession(sessionId)?.getLastAccessedTime()
    }

    @Override
    public String getLastAccessedTime(String sessionId) {
        Session session = findSessionInternal(sessionId);
        return session != null ? new Date(session.getLastAccessedTime()).toString() : "";
        //            return findSession(sessionId)?.getCreationTime()
    }

    @Override
    public String getSessionAttribute(String sessionId, String key) {
        return getCassandraOperations().getAttribute(sessionId, key).toString();
        //            return findSession(sessionId)?.getAttribute(key)
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
        if (StringUtils.isBlank(id)) {
            log.warn("create session with blank sessionId");
            id = generateSessionId();
        }
        session.setId(id, false);
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

}
