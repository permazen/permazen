
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.parse;

import com.google.common.base.Preconditions;

import io.permazen.cli.Session;
import io.permazen.core.ObjId;

/**
 * Parses object IDs.
 */
public class ObjIdParser implements Parser<ObjId> {

    public static final int MAX_COMPLETE_OBJECTS = 100;

    @Override
    public ObjId parse(Session session, String text) {

        // Sanity check
        Preconditions.checkArgument(session != null, "null session");
        Preconditions.checkArgument(text != null, "null text");

        // Attempt to parse id
        return new ObjId(text);
    }
}
