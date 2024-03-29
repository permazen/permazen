
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.test.TestSupport;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayDimensionTest extends TestSupport {

    private final DefaultEncodingRegistry registry = new DefaultEncodingRegistry();

    @Test
    public void testArrayDimensions() throws Exception {
        for (String base : new String[] { "Date", "int" }) {
            EncodingId encodingId = EncodingIds.builtin(base);
            Encoding<?> previous = registry.getEncoding(encodingId);
            for (int dims = 1; dims <= Encoding.MAX_ARRAY_DIMENSIONS; dims++) {
                encodingId = encodingId.getArrayId();
                Assert.assertEquals(encodingId.getArrayDimensions(), dims);
                final Encoding<?> encoding = registry.getEncoding(encodingId);
                assert encoding != null : "didn't find \"" + encodingId + "\"";
                assert encoding instanceof NullSafeEncoding;
                final NullSafeEncoding<?> nullSafeType = (NullSafeEncoding<?>)encoding;
                assert nullSafeType.getInnerType() instanceof ArrayEncoding;
                final ArrayEncoding<?, ?> arrayType = (ArrayEncoding<?, ?>)nullSafeType.getInnerType();
                assert arrayType.getElementType().equals(previous);
                previous = encoding;
            }
        }
    }
}
