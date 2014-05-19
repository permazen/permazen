
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import org.jsimpledb.core.FieldType;

public abstract class TransformItemsChannel<F, T> extends AbstractChannel<T> {

    private final Channel<? extends F> input;

    protected TransformItemsChannel(Channel<? extends F> input, Class<T> type) {
        this(input, new SimpleItemType<T>(type));
    }

    protected TransformItemsChannel(Channel<? extends F> input, TypeToken<T> typeToken) {
        this(input, new SimpleItemType<T>(typeToken));
    }

    protected TransformItemsChannel(Channel<? extends F> input, FieldType<T> fieldType) {
        this(input, new FieldTypeItemType<T>(fieldType));
    }

    protected TransformItemsChannel(Channel<? extends F> input, ItemType<T> toType) {
        super(toType);
        if (input == null)
            throw new IllegalArgumentException("null input");
        this.input = input;
    }

    @Override
    public Iterable<T> getItems(final Session session) {
        return Iterables.transform(this.input.getItems(session), new Function<F, T>() {
            @Override
            public T apply(F item) {
                return TransformItemsChannel.this.transformItem(session, item);
            }
        });
    }

    protected abstract T transformItem(Session session, F item);
}

