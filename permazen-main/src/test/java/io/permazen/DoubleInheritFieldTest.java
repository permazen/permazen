
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenType;

import java.util.Date;

import org.testng.annotations.Test;

public class DoubleInheritFieldTest extends MainTestSupport {

    @Test
    public void testDoubleInherit1() throws Exception {
        BasicTest.newPermazen(Foo1.class);
    }

    @Test
    public void testDoubleInherit2() throws Exception {
        try {
            BasicTest.newPermazen(Foo2.class);
            assert false : "expected error here";
        } catch (IllegalArgumentException e) {
            this.log.debug("got expected {}", e.toString());
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
    @PermazenType
    public abstract static class Foo1 implements Iface1, Iface2 {
    }

    // This should fail
    @PermazenType
    public abstract static class Foo2 implements Iface1, Iface3 {
    }
}
