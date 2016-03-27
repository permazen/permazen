
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.reflect.TypeToken;

import org.jsimpledb.parse.ParseSession;

/**
 * Support superclass for {@link Node}s that perform type inferrence and therefore require a target type
 * in order to be completely parsed.
 */
public abstract class TypeInferringNode implements Node {

    /**
     * Evaluate this node.
     *
     * <p>
     * The implementation in {@link TypeInferringNode} always throws an {@link EvalException}.
     *
     * @param session parse session
     * @return result of node evaluation
     */
    @Override
    public Value evaluate(ParseSession session) {
        throw new EvalException("cannot directly evaluate type-inferring expression");
    }

    /**
     * Resolve this instance into an {@linkplain #evaluate evaluable} {@link Node}, given the specified target type.
     *
     * <p>
     * There will be a transaction open.
     *
     * @param session parse session
     * @param target expected target type
     * @param <T> target node type
     * @return this node resolved to {@code target}
     * @throws EvalException if this instance cannot be resolved to {@code target}
     * @throws EvalException if resolution to {@code target} is ambiguous
     */
    public abstract <T> Node resolve(ParseSession session, TypeToken<T> target);
}

