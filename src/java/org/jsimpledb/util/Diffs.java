
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;

/**
 * Used to report difference(s) between two hierarchical structures.
 *
 * <p>
 * Each individual difference is a {@link String} (map key) and has optional further detail contained in another
 * {@link Diffs} instance (map value, or null for none). Keys must not be null.
 * </p>
 */
@SuppressWarnings("serial")
public class Diffs extends LinkedHashMap<String, Diffs> {

    /**
     * Default constructor.
     */
    public Diffs() {
    }

    /**
     * Copy constructor. Performs a shallow copy.
     *
     * @param other instance to copy
     */
    public Diffs(Diffs other) {
        super(other);
    }

    /**
     * Add a difference without any sub-differences.
     *
     * @param description description of the difference
     * @throws IllegalArgumentException if {@code description} is null
     */
    public void add(String description) {
        this.add(description, null);
    }

    /**
     * Add a difference with optional sub-differences.
     *
     * @param description description of the difference
     * @param diffs sub-differences, or null for none
     * @throws IllegalArgumentException if {@code description} is null
     */
    public void add(String description, Diffs diffs) {
        if (description == null)
            throw new IllegalArgumentException("null description");
        this.put(description, diffs);
    }

    /**
     * Format this instance as a {@link String}.
     */
    @Override
    public String toString() {
        if (this.isEmpty())
            return "no differences";
        final StringBuilder buf = new StringBuilder();
        this.format(buf, 0);
        return buf.toString().trim();
    }

    private void format(StringBuilder buf, int depth) {
        final String indent = this.getIndent(depth);
        for (Map.Entry<String, Diffs> entry : this.entrySet()) {
            final String description = entry.getKey();
            buf.append(String.format("%s%s%n", indent,
              description.trim().replaceAll("(\\r?\\n)", "$1" + Matcher.quoteReplacement(indent))));
            final Diffs child = entry.getValue();
            if (child != null)
                child.format(buf, depth + 1);
        }
    }

    private String getIndent(int depth) {
        final char[] indent = new char[depth * 2];
        Arrays.fill(indent, ' ');
        return new String(indent) + "-> ";
    }
}

