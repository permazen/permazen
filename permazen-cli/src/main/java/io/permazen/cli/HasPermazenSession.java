
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

/**
 * Implemented by our customized JCT objects that can provide access to the current Permazen {@link Session}.
 */
public interface HasPermazenSession {

    /**
     * Get the Permazen {@link Session} associated with this instance.
     */
    Session getPermazenSession();
}
