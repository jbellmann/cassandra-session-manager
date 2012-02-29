package de.jbellmann.tomcat.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.catalina.Container;
import org.apache.catalina.Loader;
import org.apache.catalina.Session;
import org.apache.catalina.session.ManagerBase;
import org.apache.catalina.session.StandardSession;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

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

    protected CassandraOperations cassandraOperations;

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

    CassandraOperations getCassandraOperations() {
        return this.cassandraOperations;
    }

    protected void setCassandraOperations(CassandraOperations cassandraOperations) {

    }

    //    public  void setDefaultQosParameters(QosParameters qos) {
    //            riak.setDefaultQosParameters(qos)
    //          }

    //    QosParameters getDefaultQosParameters() {
    //            return riak.getDefaultQosParameters()
    //          }

    //    RiakTemplate getRiakTemplate() {
    //            return riak
    //          }

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
        //        return container?.loader?.classLoader ?: getClass().getClassLoader()
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
    public void unload() {
        // NO-OP
    }

    @Override
    public void add(Session session) {
        // maybe we can do this with only on server-roundtrip
        getCassandraOperations().setLastAccessedTime(session.getId(), System.currentTimeMillis());
        getCassandraOperations().setCreationTime(session.getId(), System.currentTimeMillis());
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
        getCassandraOperations().removeSession(session);
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
        Session session = null;
        try {
            session = findSession(sessionId);
        } catch (IOException e) {
            log.error("IOException on CassandraManager.getCreationTime()-method with sessionId " + sessionId, e);
        }
        if (session != null) {
            return new Date(session.getCreationTime()).toString();
        }
        return "";
        //            return findSession(sessionId)?.getCreationTime()
    }

    @Override
    public long getCreationTimestamp(String sessionId) {
        Session session = findSessionInternal(sessionId);
        if (session != null) {
            return session.getCreationTime();
        } else {
            return -1;
        }
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
    public Session createSession(String sessionId) {
        CassandraSession session = (CassandraSession) createEmptySession();
        session.setNew(true);
        session.setValid(true);
        session.setMaxInactiveInterval(maxInactiveInterval);
        if (sessionId == null) {
            session.setIdInternal(generateSessionId());
            session.setCreationTime(System.currentTimeMillis());
            session.setLastAccessedTime(System.currentTimeMillis());
            sessionCounter++;
        }
        return session;
    }

}
