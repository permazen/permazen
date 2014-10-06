
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Utility methods for {@link Enum}s.
 */
public final class EnumUtil {

    private EnumUtil() {
    }

    /**
     * Get all instances of the given {@link Enum} class in a list in their natural ordering.
     *
     * @return unmodifiable list of enum values
     */
    @SuppressWarnings("unchecked")
    public static <T extends Enum<?>> List<T> getValues(Class<T> enumClass) {

        // Generate ClassCastException if type is not an enum type
        enumClass.asSubclass(Enum.class);

        // Get values
        Object array;
        try {
            array = enumClass.getMethod("values").invoke(null);
        } catch (Exception e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return Collections.unmodifiableList(Arrays.asList((T[])array));
    }
}

