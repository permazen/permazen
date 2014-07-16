
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import org.jsimpledb.cli.Session;

public class AssignmentExprParser extends BinaryExprParser {

    public static final AssignmentExprParser INSTANCE = new AssignmentExprParser();

    public AssignmentExprParser() {
        super(ConditionalParser.INSTANCE, false, Op.EQUALS, Op.PLUS_EQUALS, Op.MINUS_EQUALS, Op.MULTIPLY_EQUALS,
          Op.DIVIDE_EQUALS, Op.MODULO_EQUALS, Op.AND_EQUALS, Op.XOR_EQUALS, Op.OR_EQUALS, Op.LSHIFT_EQUALS,
          Op.RSHIFT_EQUALS, Op.URSHIFT_EQUALS);
    }

    @Override
    protected Node createNode(final Op op, final Node lhs, final Node rhs) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {

                // Get left-hand value
                final Value oldValue = lhs.evaluate(session);
                final Setter setter = oldValue.getSetter();
                if (setter == null)
                    throw new EvalException("invalid assignment to non-assignable value");

                // Calculate new value
                Value newValue = rhs.evaluate(session);
                if (op != Op.EQUALS)
                    newValue = Op.forSymbol(op.getSymbol().replaceAll("=", "")).apply(session, oldValue, newValue);

                // Assign new value
                setter.set(session, newValue);

                // Done
                return newValue;
            }
        };
    }
}

