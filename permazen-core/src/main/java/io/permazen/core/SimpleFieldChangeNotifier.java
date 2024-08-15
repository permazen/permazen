
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import java.util.NavigableSet;

final class SimpleFieldChangeNotifier<V> extends FieldChangeNotifier<SimpleFieldChangeListener, SimpleField<V>> {

    final V oldValue;
    final V newValue;

    SimpleFieldChangeNotifier(SimpleField<V> field, ObjId id, V oldValue, V newValue) {
        super(field, id);
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    public Class<SimpleFieldChangeListener> getListenerType() {
        return SimpleFieldChangeListener.class;
    }

    @Override
    public void notify(Transaction tx, SimpleFieldChangeListener listener, int[] path, NavigableSet<ObjId> referrers) {
        listener.onSimpleFieldChange(tx, this.id, this.field, path, referrers, this.oldValue, this.newValue);
    }
}
