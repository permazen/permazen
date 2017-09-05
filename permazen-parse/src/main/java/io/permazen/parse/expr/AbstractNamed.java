
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import com.google.common.base.Preconditions;

import java.util.regex.Pattern;

import io.permazen.parse.ParseUtil;

/**
 * Support superclass for things with a name.
 */
public abstract class AbstractNamed {

    protected final String name;

    /**
     * Constructor.
     *
     * @param name variable name
     * @throws IllegalArgumentException if name is null
     * @throws IllegalArgumentException if name is not a valid Java identifier
     */
    protected AbstractNamed(String name) {
        Preconditions.checkArgument(name != null, "null name");
        if (!Pattern.compile(ParseUtil.IDENT_PATTERN).matcher(name).matches())
            throw new IllegalArgumentException("invalid identifier `" + name + "'");
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + this.name + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final AbstractNamed that = (AbstractNamed)obj;
        return this.name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode();
    }
}

