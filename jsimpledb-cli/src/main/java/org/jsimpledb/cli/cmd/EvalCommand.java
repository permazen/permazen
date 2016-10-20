
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.Map;

import org.jsimpledb.Session;
import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.util.ParseContext;

public class EvalCommand extends AbstractCommand {

    public EvalCommand() {
        super("eval -f:force expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Evaluates the specified Java expression";
    }

    @Override
    public String getHelpDetail() {
        return "The expression is evaluated within a transaction. If an exception occurs, the transaction is rolled back"
          + " unless the `-f' flag is given, in which case it will be committed anyway.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public EvalAction getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        final boolean force = params.containsKey("force");
        return new EvalAction(expr, force);
    }

// EvalAction

    /**
     * Special transactional {@link CliSession.Action} used by the {@link EvalCommand} allowing access to the
     * {@link EvalException} that occurred, if any.
     *
     * <p>
     * This class is needed because {@link EvalCommand} catches and handles {@link EvalException}s
     * thrown during expression evaluation itself, swallowing them, making it appear as if the
     * command were always successful.
     */
    public static final class EvalAction implements CliSession.Action, Session.TransactionalAction {

        private final Node expr;
        private final boolean force;
        private EvalException evalException;

        private EvalAction(Node expr, boolean force) {
            this.expr = expr;
            this.force = force;
        }

        @Override
        public void run(CliSession session) throws Exception {
            final PrintWriter writer = session.getWriter();
            final Value value;
            final Object result;
            try {
                result = (value = expr.evaluate(session)).get(session);
            } catch (EvalException e) {
                this.evalException = e;
                if (!force && session.getMode().hasCoreAPI())
                    session.getTransaction().setRollbackOnly();
                writer.println(session.getErrorMessagePrefix() + e.getMessage());
                if (session.isVerbose())
                    e.printStackTrace(writer);
                return;
            }
            if (value != Value.NO_VALUE)
                writer.println(result);
        }

        /**
         * Get the {@link EvalException} that occurred when evaluating the expression, if any.
         *
         * @return exception thrown during evaluation, or null if evaluation was successful
         */
        public EvalException getEvalException() {
            return this.evalException;
        }
    }
}
