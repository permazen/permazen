
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

abstract class SetFieldNotifier<E> extends FieldChangeNotifier<SetFieldChangeListener, SetField<E>> {

    SetFieldNotifier(SetField<E> field, ObjId id) {
        super(field, id);
    }

    @Override
    public Class<SetFieldChangeListener> getListenerType() {
        return SetFieldChangeListener.class;
    }
}
