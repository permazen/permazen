
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

/**
 * Implemented by Permazen's JCT sessions to provide access to the current Permazen {@link Session}.
 */
public interface PermazenConsoleSession {

    /**
     * Get the Permazen {@link Session} associated with this JCT console session.
     */
    Session getPermazenSession();
}
