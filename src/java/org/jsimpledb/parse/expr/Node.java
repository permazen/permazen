
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
     */
    Value evaluate(ParseSession session);
}

