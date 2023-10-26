
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.DefaultFieldTypeRegistry;
import io.permazen.core.EncodingId;
import io.permazen.core.EncodingIds;
import io.permazen.core.FieldType;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayDimensionTest extends CoreAPITestSupport {

    private final DefaultFieldTypeRegistry registry = new DefaultFieldTypeRegistry();

    @Test
    public void testArrayDimensions() throws Exception {
        for (String base : new String[] { "Date", "int" }) {
            EncodingId encodingId = EncodingIds.builtin(base);
            FieldType<?> previous = registry.getFieldType(encodingId);
            for (int dims = 1; dims <= FieldType.MAX_ARRAY_DIMENSIONS; dims++) {
                encodingId = encodingId.getArrayId();
                Assert.assertEquals(encodingId.getArrayDimensions(), dims);
                final FieldType<?> fieldType = registry.getFieldType(encodingId);
                assert fieldType != null : "didn't find \"" + encodingId + "\"";
                assert fieldType instanceof NullSafeType;
                final NullSafeType<?> nullSafeType = (NullSafeType<?>)fieldType;
                assert nullSafeType.getInnerType() instanceof ArrayType;
                final ArrayType<?, ?> arrayType = (ArrayType<?, ?>)nullSafeType.getInnerType();
                assert arrayType.getElementType() == previous;
                previous = fieldType;
            }
        }
    }
}
