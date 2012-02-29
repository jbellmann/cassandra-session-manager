package de.jbellmann.tomcat.cassandra;

import java.util.Enumeration;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;

public class CassandraSession extends StandardSession {

    private static final long serialVersionUID = 1L;

    public CassandraSession(Manager manager) {
        super(manager);
    }

    public void setIdInternal(String id) {
        this.id = id;
    }

    @Override
    public void setCreationTime(long time) {
        getCassandraSessionOperations().setCreationTime(getId(), time);
    }

    public void setLastAccessedTime(long time) {
        getCassandraSessionOperations().setLastAccessedTime(getId(), time);
        //            def metadata = getRiakTemplate().get(manager.name, id)
        //            metadata.lastAccessedTime = time
        //            getRiakTemplate().set(manager.name, id, metadata)
    }

    @Override
    public long getLastAccessedTime() {
        return getCassandraSessionOperations().getLastAccessedTime(getId());
        //            def metadata = getRiakTemplate().get(manager.name, id)
        //            return this.
    }

    @Override
    public long getLastAccessedTimeInternal() {
        return super.getLastAccessedTime();
    }

    @Override
    public HttpSession getSession() {
        return new CassandraSessionFacade(this);
    }

    @Override
    public boolean isValid() {
        if (!this.isValid) {
            return false;
        }

        if (maxInactiveInterval >= 0) {
            if (maxInactiveInterval >= 0) {
                long timeNow = System.currentTimeMillis();
                int timeIdle = (int) ((timeNow - getLastAccessedTime()) / 1000L);
                if (timeIdle >= maxInactiveInterval) {
                    expire(true);
                }
            }
        }

        return this.isValid;
    }

    @Override
    public long getCreationTime() {
        return getCassandraSessionOperations().getCreationTime(getId());
        //            try {
        //              def metadata = getRiakTemplate().get(manager.name, id)
        //              return metadata.creationTime
        //            } catch (ResourceAccessException notFound) {
        //              return null;
        //            }
    }

    @Override
    public void removeAttribute(String name) {
        getCassandraSessionOperations().removeAttribute(getId(), name);
        //            getRiakTemplate().delete(getId(), name)
    }

    @Override
    public void removeAttribute(String name, boolean notify) {
        removeAttribute(name);
        //            removeAttribute(getId(), name);
    }

    @Override
    public void setAttribute(String name, Object value) {
        getCassandraSessionOperations().setAttribute(getId(), name, value);
        //            getRiakTemplate().set(getId(), name, value)
    }

    @Override
    public void setAttribute(String name, Object value, boolean notify) {
        setAttribute(name, value);
    }

    @Override
    public Object getAttribute(String name) {
        return getCassandraSessionOperations().getAttribute(getId(), name);
        //            getRiakTemplate().get(getId(), name)
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return getCassandraSessionOperations().getAttributeNames(getId());
        //            def schema = getRiakTemplate().getBucketSchema(getId(), true)
        //            return Collections.enumeration(schema.keys)
    }

    @Override
    protected String[] keys() {
        return getCassandraSessionOperations().keys(getId());
        //            Map<String, Object> schema = getRiakTemplate().getBucketSchema(manager.name, id, true);
        //            return schema.keys;
    }

    //          RiakTemplate getRiakTemplate() {
    //            return ((RiakManager) manager).getRiakTemplate()
    //          }

    CassandraOperations getCassandraSessionOperations() {
        return ((CassandraManager) manager).getCassandraOperations();
    }
}
