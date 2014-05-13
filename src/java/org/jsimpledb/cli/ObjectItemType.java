
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import java.io.IOException;

import org.jsimpledb.core.ObjId;

public class ObjectItemType implements DeletableItemType<ObjId> {

    @Override
    public TypeToken<ObjId> getTypeToken() {
        return TypeToken.of(ObjId.class);
    }

    @Override
    public void print(Session session, ObjId id) throws IOException {
        final Util.ObjInfo info = Util.getObjInfo(session, id);
        session.getWriter().println(info != null ? info.toString() : id + " DELETED");
    }

    @Override
    public boolean delete(Session session, ObjId id) {
        return session.getTransaction().delete(id);
    }
}

