package de.jbellmann.tomcat.cassandra.example.controller;

import java.io.Serializable;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class Result implements Serializable {

    private static final long serialVersionUID = 1L;

    private int value;

    public Result() {
    }

    public Result(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Result[value=" + getValue() + "]";
    }
}
