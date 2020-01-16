
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.Permazen;
import io.permazen.Session;
import io.permazen.ValidationMode;
import io.permazen.annotation.PermazenType;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ExprParser;
import io.permazen.test.TestSupport;
import io.permazen.util.ParseContext;

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
        final Permazen jdb = new Permazen(Parent.class, Child.class);
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

    @Test
    public void testAllParam() throws Exception {
        this.testExpression("java.util.stream.Stream.of(Object.class).map(c -> all(c)).count()", 1L);
    }

    private void testExpression(final String expression, Object expected) {
        final Object[] actual = new Object[1];
        this.session.performParseSessionAction((TestAction)session2 -> actual[0]
          = new ExprParser().parse(session2, new ParseContext(expression), false).evaluate(session2).get(session2));
        Assert.assertEquals(actual[0], expected);
    }

    private interface TestAction extends ParseSession.Action, Session.TransactionalAction {
    }

// Model classes

    public interface HasName {
        String getName();
        void setName(String name);
    }

    @PermazenType
    public abstract static class Parent implements JObject, HasName {
    }

    @PermazenType
    public abstract static class Child implements JObject, HasName {

        public abstract Parent getParent();
        public abstract void setParent(Parent parent);
    }
}

