
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import io.permazen.test.TestSupport;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.annotations.Test;

public class CloseableRefsTest extends TestSupport {

    @Test
    public void testCloseableRefs() {
        final AtomicBoolean closed = new AtomicBoolean();
        final Closeable c = () -> closed.set(true);
        final CloseableRefs<?> refs = new CloseableRefs<>(c);
        assert !closed.get();
        final int limit = this.random.nextInt(25);
        for (int i = 0; i < limit; i++) {
            refs.ref();
            assert !closed.get();
        }
        for (int i = 0; i < limit; i++) {
            refs.unref();
            assert !closed.get();
        }
        refs.unref();
        assert closed.get();
    }
}
