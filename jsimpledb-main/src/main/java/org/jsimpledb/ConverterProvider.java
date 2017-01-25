
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;

interface ConverterProvider {

    /**
     * Get a {@link Converter} that converts from what the core database returns to what the Java application expects.
     *
     * @param jtx transaction
     * @return {@link Converter} from core API to Java
     */
    Converter<?, ?> getConverter(JTransaction jtx);

    static ConverterProvider identityForNull(ConverterProvider provider) {
        return jtx -> {
            final Converter<?, ?> converter = provider.getConverter(jtx);
            return converter != null ? converter : Converter.identity();
        };
    }
}
