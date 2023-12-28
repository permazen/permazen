
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.PermazenType;
import io.permazen.index.Index1;

import org.testng.annotations.Test;

public class RawTypeTest extends MainTestSupport {

    @Test
    public void testRawType() throws Exception {
        final Permazen jdb = BasicTest.newPermazen(Widget.class);
        final JTransaction jtx = jdb.createTransaction(ValidationMode.MANUAL);
        JTransaction.setCurrent(jtx);
        try {
            AbstractData.queryByName();
        } finally {
            JTransaction.setCurrent(null);
        }
    }

// Model Classes

    @PermazenType(storageId = 100)
    public abstract static class AbstractData<T extends AbstractData<T>> implements JObject {

        @JField(storageId = 201, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @SuppressWarnings("rawtypes")
        public static Index1<String, AbstractData> queryByName() {
            return JTransaction.getCurrent().querySimpleIndex(AbstractData.class, "name", String.class);
        }
    }

    @PermazenType(storageId = 400)
    public abstract static class Widget extends AbstractData<Widget> {
    }
}
