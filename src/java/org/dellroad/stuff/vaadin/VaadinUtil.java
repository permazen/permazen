
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

/**
 * Miscellaneous utility methods.
 */
public final class VaadinUtil {

    private VaadinUtil() {
    }

    /**
     * Verify that we are running in the context of the given {@link ContextApplication} and that it is locked.
     * This method can be used by any code that manipulates Vaadin state to assert that the proper Vaadin
     * locking has been performed.
     *
     * @param application the {@link ContextApplication} we are supposed to be running with
     * @throws IllegalArgumentException if {@code application} is null
     * @throws IllegalStateException if there is no {@link ContextApplication} associated with the current thread
     * @throws IllegalStateException if the {@link ContextApplication} associated with the current thread is not {@code application}
     * @throws IllegalStateException if the {@link ContextApplication} associated with the current thread is not locked
     */
    public static void assertApplication(ContextApplication application) {
        if (application == null)
            throw new IllegalArgumentException("null application");
        final ContextApplication currentApplication = ContextApplication.currentApplication();
        if (currentApplication == null)
            throw new IllegalStateException("there is no ContextApplication associated with the current thread");
        if (currentApplication != application) {
            throw new IllegalStateException("the ContextApplication associated with the current thread " + currentApplication
              + " is not the same application as the given one " + application);
        }
        if (!Thread.holdsLock(application)) {
            throw new IllegalStateException("the ContextApplication associated with the current thread " + currentApplication
              + " is not locked by this thread");
        }
    }
}

