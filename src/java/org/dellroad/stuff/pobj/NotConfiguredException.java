
/*
 * Copyright (C) 2012 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

/**
 * Thrown by {@link AbstractConfiguredBean#getRequiredConfig} when the bean is not configured.
 */
@SuppressWarnings("serial")
public class NotConfiguredException extends IllegalStateException {

    public NotConfiguredException() {
    }

    public NotConfiguredException(Throwable t) {
        super(t);
    }

    public NotConfiguredException(String message) {
        super(message);
    }

    public NotConfiguredException(String message, Throwable t) {
        super(message, t);
    }
}

