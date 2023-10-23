
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import io.permazen.core.CoreAPITestSupport;
import io.permazen.core.FieldType;
import io.permazen.core.FieldTypeRegistry;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ArrayDimensionTest extends CoreAPITestSupport {

    private final FieldTypeRegistry registry = new FieldTypeRegistry();

    @Test
    public void testArrayDimensions() throws Exception {
        for (String base : new String[] { "java.util.Date", "int" }) {
            final StringBuilder buf = new StringBuilder(base);
            for (int dims = 1; dims <= ArrayType.MAX_DIMENSIONS; dims++) {
                buf.append("[]");
                final String typeName = buf.toString();
                final FieldType<?> fieldType = registry.getFieldType(typeName);
                assert fieldType != null : "didn't find \"" + typeName + "\"";
                assert fieldType instanceof NullSafeType;
                final NullSafeType<?> nullSafeType = (NullSafeType<?>)fieldType;
                assert nullSafeType.getInnerType() instanceof ArrayType;
                final ArrayType<?, ?> arrayType = (ArrayType<?, ?>)nullSafeType.getInnerType();
                Assert.assertEquals(arrayType.getDimensions(), dims);
            }
        }
    }
}
