
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.Session;
import io.permazen.SessionMode;
import io.permazen.cli.CliSession;
import io.permazen.parse.expr.EvalException;
import io.permazen.parse.expr.Node;
import io.permazen.parse.expr.Value;
import io.permazen.util.ParseContext;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

public class EvalCommand extends AbstractCommand {

    public EvalCommand() {
        super("eval -f:force -w:weak expr:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Evaluates the specified Java expression";
    }

    @Override
    public String getHelpDetail() {
        return "The expression is evaluated within a transaction. If an exception occurs, the transaction is rolled back"
          + " unless the `-f' flag is given, in which case it will be committed anyway.\n"
          + "If the `-w' flag is given, for certain key/value stores a weaker consistency level is used for"
          + " the tranasction to reduce the chance of conflicts.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public EvalAction getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {
        final Node expr = (Node)params.get("expr");
        final boolean force = params.containsKey("force");
        final boolean weak = params.containsKey("weak");
        return new EvalAction(expr, force, weak);
    }

// EvalAction

    /**
     * Special transactional {@link io.permazen.cli.CliSession.Action} used by the {@link EvalCommand} allowing access to the
     * {@link EvalException} that occurred, if any.
     *
     * <p>
     * This class is needed because {@link EvalCommand} catches and handles {@link EvalException}s
     * thrown during expression evaluation itself, swallowing them, making it appear as if the
     * command were always successful.
     */
    public static final class EvalAction implements CliSession.Action, Session.TransactionalAction, Session.HasTransactionOptions {

        private final Node expr;
        private final boolean force;
        private final boolean weak;
        private EvalException evalException;

        private EvalAction(Node expr, boolean force, boolean weak) {
            this.expr = expr;
            this.force = force;
            this.weak = weak;
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

        // Use EVENTUAL_COMMITTED consistency for Raft key/value stores to avoid retries
        @Override
        public Map<String, ?> getTransactionOptions() {
            return this.weak ? Collections.singletonMap("consistency", "EVENTUAL") : null;
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
