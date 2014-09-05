
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.util.NavigableMap;
import java.util.NavigableSet;

import org.jsimpledb.annotation.IndexQuery;
import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.testng.annotations.Test;

public class RawTypeTest extends TestSupport {

    @Test
    public void testRawType() throws Exception {
        BasicTest.getJSimpleDB(Widget.class);
    }

// Model Classes

    @JSimpleClass(storageId = 100)
    public abstract static class AbstractData<T extends AbstractData<T>> implements JObject {

        @JField(storageId = 201, indexed = true)
        public abstract String getName();
        public abstract void setName(String name);

        @IndexQuery(type = AbstractData.class, value = "name")
        protected abstract NavigableMap<String, NavigableSet<AbstractData<?>>> queryByName();
    }

    @JSimpleClass(storageId = 400)
    public abstract static class Widget extends AbstractData<Widget> {
    }
}

