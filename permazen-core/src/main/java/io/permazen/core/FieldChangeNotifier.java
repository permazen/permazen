
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

/**
 * Superclass for field change notifiers.
 *
 * @param <L> the listener type that is the target of the notification
 * @param <F> field type
 */
abstract class FieldChangeNotifier<L, F extends Field<?>> extends Notifier<L> {

    protected final F field;

    FieldChangeNotifier(F field, ObjId id) {
        super(id);
        assert field != null;
        this.field = field;
    }
}
