
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

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

