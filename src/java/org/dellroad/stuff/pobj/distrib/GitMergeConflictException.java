
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj.distrib;

/**
 * Exception thrown by a {@link GitRepository} when a merge fails due to conflicts.
 */
@SuppressWarnings("serial")
public class GitMergeConflictException extends GitException {

    public GitMergeConflictException(String message) {
        super(message);
    }

    public GitMergeConflictException(String message, Throwable cause) {
        super(message, cause);
    }
}

