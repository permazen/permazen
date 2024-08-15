
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

abstract class ListFieldNotifier<E> extends FieldChangeNotifier<ListFieldChangeListener, ListField<E>> {

    ListFieldNotifier(ListField<E> field, ObjId id) {
        super(field, id);
    }

    @Override
    public Class<ListFieldChangeListener> getListenerType() {
        return ListFieldChangeListener.class;
    }
}
