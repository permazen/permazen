
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.collect.Iterators;

import io.permazen.JObject;
import io.permazen.Session;
import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.ExprParser;
import io.permazen.parse.expr.Node;
import io.permazen.util.CastFunction;
import io.permazen.util.ParseContext;

import java.util.Collections;
import java.util.Iterator;

/**
 * {@link QueryJObjectContainer} whose query is defined by a Java expression.
 */
@SuppressWarnings("serial")
public class ExprQueryJObjectContainer extends QueryJObjectContainer {

    protected final ParseSession session;

    private String contentExpression;

    /**
     * Constructor.
     *
     * @param session session for parsing expressions
     */
    public ExprQueryJObjectContainer(ParseSession session) {
        this(session, null);
    }

    /**
     * Constructor.
     *
     * @param session session for parsing expressions
     * @param type type restriction, or null for no restriction
     */
    public ExprQueryJObjectContainer(ParseSession session, Class<?> type) {
        super(session.getJSimpleDB(), type);
        this.session = session;
    }

    /**
     * Configure the expression that returns this container's content. Container will automatically reload.
     *
     * @param contentExpression Java expression for container content
     */
    public void setContentExpression(String contentExpression) {
        this.contentExpression = contentExpression;
        this.reload();
    }

    // Ensure all transactions are run in association with the session
    @Override
    protected void doInTransaction(final Runnable action) {
        session.performParseSessionAction((RetryableParseAction)session2 -> action.run());
    }

    private interface RetryableParseAction extends ParseSession.Action, Session.RetryableAction {
    }

    @Override
    protected Iterator<? extends JObject> queryForObjects() {

        // Any content?
        if (this.contentExpression == null)
            return Collections.<JObject>emptyIterator();

        // Parse expression
        final ParseContext ctx = new ParseContext(this.contentExpression);
        final Node node = new ExprParser().parse(this.session, ctx, false);
        ctx.skipWhitespace();
        if (!ctx.isEOF())
            throw new ParseException(ctx, "syntax error");

        // Evaluate parsed expression
        final Object content = node.evaluate(this.session).get(this.session);
        final Iterator<?> iterator;
        if (content instanceof Iterator)
            iterator = (Iterator<?>)content;
        else if (content instanceof Iterable)
            iterator = ((Iterable<?>)content).iterator();
        else {
            throw new EvalException("expression must evaluate to an Iterable or Iterator; found "
              + (content != null ? content.getClass().getName() : "null") + " instead");
        }

        // Reload container with results of expression
        return Iterators.transform(iterator, new CastFunction<JObject>(JObject.class).toGuava());
    }
}

