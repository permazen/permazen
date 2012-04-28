
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.util.HashSet;
import java.util.concurrent.Callable;

import org.dellroad.stuff.TestSupport;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ThreadLocalHolderTest extends TestSupport {

    @Test
    public void testThreadLocalHolder() throws Exception {

    // Setup

        final HashSet<Object> destroyed = new HashSet<Object>();

        final ThreadLocalHolder<Object> t = new ThreadLocalHolder<Object>() {
            @Override
            protected void destroy(Object obj) {
                destroyed.add(obj);
            }
        };

    // Check not executing

        Assert.assertNull(t.get());

        try {
            t.require();
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }

        assert destroyed.isEmpty();

    // Test Runnable

        final Object foo = new Object();

        Runnable rable = new Runnable() {
            @Override
            public void run() {
                Object current = t.require();
                assert current == foo;
                current = t.get();
                assert current == foo;
                assert destroyed.isEmpty();
            }
        };

        t.invoke(foo, rable);

        assert destroyed.iterator().next() == foo;

        destroyed.clear();

    // Test Callable

        final Object foo2 = new Object();
        final Object bar = new Object();

        Callable<Object> cable = new Callable<Object>() {
            @Override
            public Object call() {
                Object current = t.require();
                assert current == foo2;
                current = t.get();
                assert current == foo2;
                assert destroyed.isEmpty();
                return bar;
            }
        };

        Object bar2 = t.invoke(foo2, cable);
        assert bar2 == bar;

        assert destroyed.iterator().next() == foo2;

    // Check resetedness

        Assert.assertNull(t.get());

        try {
            t.require();
            assert false;
        } catch (IllegalStateException e) {
            // ok
        }
    }
}

