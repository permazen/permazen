
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Date;

import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.simple.SimpleKVDatabase;
import org.jsimpledb.test.TestSupport;
import org.testng.annotations.Test;

public class DoubleInheritFieldTest extends TestSupport {

    @Test
    public void testDoubleInherit1() throws Exception {
        final JSimpleDBFactory factory = new JSimpleDBFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(Foo1.class);
        factory.newJSimpleDB();
    }

    @Test
    public void testDoubleInherit2() throws Exception {
        final JSimpleDBFactory factory = new JSimpleDBFactory();
        factory.setDatabase(new Database(new SimpleKVDatabase()));
        factory.setSchemaVersion(1);
        factory.setModelClasses(Foo2.class);
        try {
            factory.newJSimpleDB();
            assert false;
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

// Model Classes

    public interface Iface1 {
        Date getCreatedOn();
        void setCreatedOn(Date createdOn);
    }

    public interface Iface2 {
        Date getCreatedOn();
        void setCreatedOn(Date createdOn);
    }

    public interface Iface3 {
        @org.jsimpledb.annotation.JField(indexed = true)
        Date getCreatedOn();
        void setCreatedOn(Date createdOn);
    }

    // This should work
    @JSimpleClass
    public abstract static class Foo1 implements Iface1, Iface2 {
    }

    // This should fail
    @JSimpleClass
    public abstract static class Foo2 implements Iface1, Iface3 {
    }
}
