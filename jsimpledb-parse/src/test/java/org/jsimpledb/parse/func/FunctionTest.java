
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.func;

import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.ParseContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class FunctionTest extends TestSupport {

    private ParseSession session;
    private Parent parent;
    private Child child1;
    private Child child2;

    @BeforeClass
    public void testFunctions() throws Exception {

        // Set up data
        final JSimpleDB jdb = new JSimpleDB(Parent.class, Child.class);
        final JTransaction jtx = jdb.createTransaction(true, ValidationMode.AUTOMATIC);
        JTransaction.setCurrent(jtx);
        try {

            this.parent = jtx.create(Parent.class);
            this.parent.setName("Parent");

            this.child1 = jtx.create(Child.class);
            this.child1.setName("child1");
            this.child1.setParent(this.parent);

            this.child2 = jtx.create(Child.class);
            this.child2.setName("child2");
            this.child2.setParent(this.parent);

            jtx.commit();

        } finally {
            JTransaction.setCurrent(null);
        }

        // Create parse session and register functions
        this.session = new ParseSession(jdb);
        this.session.setValidationMode(ValidationMode.AUTOMATIC);
        this.session.loadFunctionsFromClasspath();
    }

    @Test
    public void testQueryIndex() throws Exception {
        this.testExpression("queryIndex(Object.class, \"parent\", Object.class).asMap().get(@"
          + this.parent.getObjId() + ").size()", 2);
    }

    private void testExpression(final String expression, Object expected) {
        final Object[] actual = new Object[1];
        this.session.performParseSessionAction(new ParseSession.TransactionalAction() {
            @Override
            public void run(ParseSession session) throws Exception {
                actual[0] = new ExprParser().parse(session, new ParseContext(expression), false).evaluate(session).get(session);
            }
        });
        Assert.assertEquals(actual[0], expected);
    }

// Model classes

    public interface HasName {
        String getName();
        void setName(String name);
    }

    @JSimpleClass
    public abstract static class Parent implements JObject, HasName {
    }

    @JSimpleClass
    public abstract static class Child implements JObject, HasName {

        public abstract Parent getParent();
        public abstract void setParent(Parent parent);
    }
}

