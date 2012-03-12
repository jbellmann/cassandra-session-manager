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

import java.util.Enumeration;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpSession;
import javax.servlet.http.HttpSessionContext;

/**
 * This facade is used to prevent downcasting to a concrete implementation.
 * 
 * @author Joerg Bellmann
 *
 */
public class CassandraSessionFacade implements HttpSession {

    private final CassandraSession cassandraSession;

    CassandraSessionFacade(CassandraSession cassandraSession) {
        this.cassandraSession = cassandraSession;
    }

    @Override
    public Object getAttribute(String name) {
        return this.cassandraSession.getAttribute(name);
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return this.cassandraSession.getAttributeNames();
    }

    @Override
    public long getCreationTime() {
        return this.cassandraSession.getCreationTime();
    }

    @Override
    public String getId() {
        return this.cassandraSession.getId();
    }

    @Override
    public long getLastAccessedTime() {
        return this.cassandraSession.getLastAccessedTime();
    }

    @Override
    public int getMaxInactiveInterval() {
        return this.cassandraSession.getMaxInactiveInterval();
    }

    @Override
    public ServletContext getServletContext() {
        return this.cassandraSession.getServletContext();
    }

    @Override
    public HttpSessionContext getSessionContext() {
        return this.cassandraSession.getSessionContext();
    }

    @Override
    public Object getValue(String name) {
        return this.cassandraSession.getAttribute(name);
    }

    @Override
    public String[] getValueNames() {
        return this.cassandraSession.getValueNames();
    }

    @Override
    public void invalidate() {
        this.cassandraSession.invalidate();
    }

    @Override
    public boolean isNew() {
        return this.cassandraSession.isNew();
    }

    @Override
    public void putValue(String name, Object value) {
        this.cassandraSession.setAttribute(name, value);
    }

    @Override
    public void removeAttribute(String name) {
        this.cassandraSession.removeAttribute(name);
    }

    @Override
    public void removeValue(String name) {
        this.cassandraSession.removeAttribute(name);
    }

    @Override
    public void setAttribute(String name, Object value) {
        this.cassandraSession.setAttribute(name, value);
    }

    @Override
    public void setMaxInactiveInterval(int interval) {
        this.cassandraSession.setMaxInactiveInterval(interval);
    }

}
