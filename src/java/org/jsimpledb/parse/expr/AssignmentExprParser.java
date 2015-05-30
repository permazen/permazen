
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * Parses Java assignment expressions of the form {@code x = y}, {@code x += y}, etc.
 */
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
            public Value evaluate(ParseSession session) {

                // Get left-hand value, which must be an LValue
                final LValue lvalue = lhs.evaluate(session).asLValue("assignment");

                // Calculate new value
                Value value = rhs.evaluate(session);
                if (op != Op.EQUALS)
                    value = Op.forSymbol(op.getSymbol().replaceAll("=", "")).apply(session, lvalue, value);

                // Assign new value to lvalue
                lvalue.set(session, value);

                // Done
                return value;
            }
        };
    }
}

