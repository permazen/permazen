
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.index.Index1;

import org.testng.annotations.Test;

public class RawTypeTest extends MainTestSupport {

    @Test
    public void testRawType() throws Exception {
        final Permazen pdb = BasicTest.newPermazen(Widget.class);
        final PermazenTransaction ptx = pdb.createTransaction(ValidationMode.MANUAL);
        PermazenTransaction.setCurrent(ptx);
        try {
            AbstractData.queryByName();
        } finally {
            PermazenTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class AbstractData<T extends AbstractData<T>> implements PermazenObject {

        @PermazenField(storageId = 201, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @SuppressWarnings("rawtypes")
        public static Index1<String, AbstractData> queryByName() {
            return PermazenTransaction.getCurrent().querySimpleIndex(AbstractData.class, "name", String.class);
        }
    }

    @PermazenType(storageId = 400)
    public abstract static class Widget extends AbstractData<Widget> {
    }
}
