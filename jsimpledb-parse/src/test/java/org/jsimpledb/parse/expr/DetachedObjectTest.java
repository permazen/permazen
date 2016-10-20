
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JTransaction;
import org.jsimpledb.Session;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.test.TestSupport;
import org.jsimpledb.util.ParseContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DetachedObjectTest extends TestSupport {

    private ParseSession session;

    @BeforeClass
    public void setup() throws Exception {
        final JSimpleDB jdb = new JSimpleDB(Person.class);
        this.session = new ParseSession(jdb);
        this.session.setSchemaVersion(1);
        this.session.setAllowNewSchema(true);
        this.session.loadFunctionsFromClasspath();
    }

    @Test
    public void testDetachedObject() {

        // Create object and assign it to variable $x
        boolean success = this.session.performParseSessionAction(new TestAction() {
            @Override
            public void run(ParseSession session) {

                // Create "Fred"
                final JTransaction jtx = session.getJTransaction();
                final Person fred = jtx.create(Person.class);
                fred.setName("Fred");

                // Assign $x = fred
                new ExprParser().parse(session, new ParseContext("$x = @" + fred.getObjId()), false).evaluate(session);
            }
        });
        Assert.assertTrue(success);

        // Now dereference $x in a new transaction
        success = this.session.performParseSessionAction(new TestAction() {
            @Override
            public void run(ParseSession session) {

                // Get $x.name
                final Object name = new ExprParser().parse(session,
                  new ParseContext("$x.name"), false).evaluate(session).get(session);
                Assert.assertEquals(name, "Fred");
            }
        });
        Assert.assertTrue(success);
    }

    private interface TestAction extends ParseSession.Action, Session.TransactionalAction {
    }

// Model classes

    @JSimpleClass
    public abstract static class Person implements JObject {

        public abstract String getName();
        public abstract void setName(String name);
    }
}

