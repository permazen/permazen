
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
            EncodingId previousDimensionEncodingId = EncodingIds.builtin(base);
            Encoding<?> previousDimensionEncoding = registry.getEncoding(previousDimensionEncodingId);
            for (int dims = 1; dims <= Encoding.MAX_ARRAY_DIMENSIONS; dims++) {

                // Get encoding ID corresponding to one more array dimension
                final EncodingId arrayEncodingId = previousDimensionEncodingId.getArrayId();
                Assert.assertEquals(arrayEncodingId.getArrayDimensions(), dims);

                // Get the corresponding array encoding (it necessarily supports null)
                final Encoding<?> arrayEncoding = registry.getEncoding(arrayEncodingId);
                assert arrayEncoding != null : "didn't find \"" + arrayEncodingId + "\"";
                assert arrayEncoding instanceof NullSafeEncoding;
                assert arrayEncoding.supportsNull();

                // Extract the non-null supporting version of it, which is an ArrayEncoding
                final NullSafeEncoding<?> nullSafeType = (NullSafeEncoding<?>)arrayEncoding;
                final ArrayEncoding<?, ?> nonNullArrayEncoding = (ArrayEncoding<?, ?>)nullSafeType.getInnerEncoding();
                assert nonNullArrayEncoding instanceof ArrayEncoding;
                assert !nonNullArrayEncoding.supportsNull();

                // Get the element encoding from the array encoding
                final Encoding<?> elementEncoding = nonNullArrayEncoding.getElementEncoding();

                // Verify the non-null version of the element encoding
                final Encoding<?> nonNullElementEncoding;
                if (elementEncoding instanceof NullSafeEncoding)
                    nonNullElementEncoding = ((NullSafeEncoding<?>)elementEncoding).getInnerEncoding();
                else {
                    assert elementEncoding instanceof PrimitiveEncoding && dims == 1;
                    nonNullElementEncoding = elementEncoding;
                }

                // The element encoding should equal the previous dimension's array encoding (sans EncodingId)
                assert elementEncoding.withEncodingId(null).equals(previousDimensionEncoding.withEncodingId(null)) :
                  String.format("dims=%d elementEncoding=%s previousDimensionEncoding=%s",
                    dims, elementEncoding, previousDimensionEncoding);

                // Proceed into the next dimension
                previousDimensionEncodingId = arrayEncodingId;
                previousDimensionEncoding = arrayEncoding;
            }
        }
    }
}
