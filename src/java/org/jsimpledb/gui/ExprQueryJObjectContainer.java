
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.google.common.collect.Iterables;

import java.util.Collections;

import org.jsimpledb.JObject;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.util.CastFunction;

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
        session.performParseSessionAction(new ParseSession.TransactionalAction() {
            @Override
            public void run(ParseSession session) {
                action.run();
            }
        });
    }

    @Override
    protected Iterable<? extends JObject> queryForObjects() {

        // Any content?
        if (this.contentExpression == null)
            return Collections.<JObject>emptySet();

        // Parse expression
        final ParseContext ctx = new ParseContext(this.contentExpression);
        final Node node = new ExprParser().parse(this.session, ctx, false);
        ctx.skipWhitespace();
        if (!ctx.isEOF())
            throw new ParseException(ctx, "syntax error");

        // Evaluate parsed expression
        final Object content = node.evaluate(this.session).get(this.session);
        if (!(content instanceof Iterable)) {
            throw new EvalException("expression must evaluate to an Iterable; found "
              + (content != null ? content.getClass().getName() : "null") + " instead");
        }

        // Reload container with results of expression
        return Iterables.transform((Iterable<?>)content, new CastFunction<JObject>(JObject.class));
    }
}

