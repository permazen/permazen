
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JSimpleClass;
import io.permazen.core.Database;
import io.permazen.kv.simple.SimpleKVDatabase;
import io.permazen.test.TestSupport;

import java.util.Date;

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
        @io.permazen.annotation.JField(indexed = true)
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
