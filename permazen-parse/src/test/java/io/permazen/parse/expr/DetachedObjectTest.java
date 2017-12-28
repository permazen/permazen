
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.JObject;
import io.permazen.JTransaction;
import io.permazen.Permazen;
import io.permazen.Session;
import io.permazen.annotation.PermazenType;
import io.permazen.parse.ParseSession;
import io.permazen.test.TestSupport;
import io.permazen.util.ParseContext;

import java.util.function.Consumer;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class DetachedObjectTest extends TestSupport {

    private ParseSession session;

    @BeforeClass
    public void setup() throws Exception {
        final Permazen jdb = new Permazen(Person.class);
        this.session = new ParseSession(jdb);
        this.session.setSchemaVersion(1);
        this.session.setAllowNewSchema(true);
        this.session.loadFunctionsFromClasspath();
    }

    @Test
    public void testDetachedObject1() {

        // Create "Fred" object and assign it to variable $fred
        this.doAction(session2 -> {

            // Create "Fred"
            final JTransaction jtx = session2.getJTransaction();
            final Person fred = jtx.create(Person.class);
            fred.setName("Fred");

            // Assign $fred = fred
            this.doParse(session2, "$fred = @" + fred.getObjId());
        });

        // Now dereference $fred in a new transaction
        final Object name = this.doParse("$fred.name");
        Assert.assertEquals(name, "Fred");
    }

    //@Test     FIXME
    public void testDetachedObject2() {

        // Create "Fred" object and assign it to variable $fred
        this.doAction(session2 -> {

            // Create "Fred"
            final JTransaction jtx = session2.getJTransaction();
            final Person fred = jtx.create(Person.class);
            fred.setName("Fred");

            // Assign $fred = fred
            this.doParse(session2, "$fred = @" + fred.getObjId());
        });

        // Assign $set1 = Collections.singleton(fred)
        this.doParse("$set1 = java.util.Collections.singleton($fred)");

        // Assign $set2 = Collections.singleton(fred)
        this.doParse("$set2 = java.util.Collections.singleton($fred)");

        // Sets should be equal
        final Object result = this.doParse("$set1.equals($set2)");
        Assert.assertEquals(result, Boolean.TRUE);
    }

    private void doAction(Consumer<ParseSession> action) {
        final boolean success = this.session.performParseSessionAction((TestAction)session2 -> action.accept(session2));
        Assert.assertTrue(success);
    }

    private Object doParse(String command) {
        final Object[] result = new Object[1];
        this.doAction(session2 -> result[0] = this.doParse(session2, command));
        return result[0];
    }

    private Object doParse(ParseSession session2, String command) {
        return new ExprParser().parse(session2, new ParseContext(command), false).evaluate(session2).get(session2);
    }

    private interface TestAction extends ParseSession.Action, Session.TransactionalAction {
    }

// Model classes

    @PermazenType
    public abstract static class Person implements JObject {

        public abstract String getName();
        public abstract void setName(String name);
    }
}

