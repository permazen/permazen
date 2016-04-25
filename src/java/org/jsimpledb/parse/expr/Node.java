
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * The product of a parse operation, which is capable of producing a {@link Value} when evaluated within a transaction.
 */
public interface Node {

    /**
     * Evaluate this node. There will be a transaction open.
     *
     * @param session parse session
     * @return result of node evaluation
     * @throws EvalException if evaluation fails
     */
    Value evaluate(ParseSession session);

    /**
     * Get the type of this node's value.
     *
     * <p>
     * If the type is unknown, {@code Object.class} should be returned.
     *
     * @param session parse session
     * @return the expected type of the node
     */
    Class<?> getType(ParseSession session);
}
