
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.parse.ParseSession;

/**
 * The product of a parse operation capable of producing a value in a transaction.
 */
public interface Node {

    /**
     * Evaluate this node. There will be a transaction open.
     */
    Value evaluate(ParseSession session);
}

