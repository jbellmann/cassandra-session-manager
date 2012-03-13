package de.jbellmann.tomcat.cassandra.example.controller;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;


public class CalculationServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final String RESULT_ATTRIBUTE = "RESULT_ATTRIBUTE";

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        HttpSession session = req.getSession();
        Result result = (Result) session.getAttribute(RESULT_ATTRIBUTE);
        if (result == null) {
            result = new Result(0);
        }
        result = new Result(result.getValue() + 1);
        session.setAttribute(RESULT_ATTRIBUTE, result);
    }

}
