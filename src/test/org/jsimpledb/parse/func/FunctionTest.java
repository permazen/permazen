
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.BasicTest;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.TestSupport;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.demo.Moon;
import org.jsimpledb.demo.Planet;
import org.jsimpledb.demo.Star;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.spring.AnnotatedClassScanner;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FunctionTest extends TestSupport {

    private ParseSession session;
    private Star sun;
    private Planet earth;
    private Planet mars;
    private Moon moon;

    @BeforeClass
    public void testFunctions() throws Exception {

        // Set up data
        final JSimpleDB jdb = BasicTest.getJSimpleDB(Star.class, Planet.class, Moon.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            this.sun = jtx.create(Star.class);
            this.sun.setName("Sun");

            this.earth = jtx.create(Planet.class);
            this.earth.setName("Earth");
            this.earth.setParent(this.sun);

            this.mars = jtx.create(Planet.class);
            this.mars.setName("Mars");
            this.mars.setParent(this.sun);

            this.moon = jtx.create(Moon.class);
            this.moon.setName("Moon");
            this.moon.setParent(this.earth);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        // Create parse session and register functions
        this.session = new ParseSession(jdb);
        this.session.setValidationMode(ValidationMode.AUTOMATIC);
        for (String className : new AnnotatedClassScanner(Function.class).scanForClasses(ParseSession.class.getPackage().getName()))
            session.registerFunction(Class.forName(className, false, Thread.currentThread().getContextClassLoader()));
    }

    @Test
    public void testQueryIndex() throws Exception {
        this.testExpression("queryIndex(Object.class, \"parent\", Object.class).asMap().get(@"
          + this.earth.getObjId() + ").iterator().next().getObjId()", this.moon.getObjId());
    }

    private void testExpression(final String expression, Object expected) {
        final Object[] actual = new Object[1];
        this.session.performParseSessionAction(new ParseSession.Action() {
            @Override
            public void run(ParseSession session) throws Exception {
                actual[0] = new ExprParser().parse(session, new ParseContext(expression), false).evaluate(session).get(session);
            }
        });
        Assert.assertEquals(actual[0], expected);
    }
}

