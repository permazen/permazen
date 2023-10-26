
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Converter;

public class IntStringConverter extends Converter<Integer, String> {

    @Override
    protected String doForward(Integer i) {
        return "" + i;
    }

    @Override
    protected Integer doBackward(String s) {
        final int i = Integer.parseInt(s);
        if (!s.equals("" + i))
            throw new IllegalArgumentException("\"" + s + "\" != \"" + i + "\"");
        return i;
    }
}
