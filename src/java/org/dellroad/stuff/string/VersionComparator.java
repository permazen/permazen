
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Comparator for version numbers.
 *
 * <p>
 * Version numbers are broken up into <b>parts</b>, where a <b>part</b> is a contiguous sequence
 * of one or more digits, or a contiguous sequence of one or more non-digits, and where the period character
 * ('.') serves as a part separator. Then, parts are compared pair-wise until there is a difference, as
 * determined by the {@link Comparator} provided to the constructor, or one version string runs out of parts,
 * in which case it is less than the other.
 * </p>
 */
public class VersionComparator implements Comparator<String> {

    private static final Pattern PART_PATTERN = Pattern.compile("[0-9]+|[^0-9.]+|\\.");

    private final Comparator<String> partComparator;

    /**
     * Primary constructor.
     *
     * @param partComparator compares individual parts
     * @throws IllegalArgumentException if {@code partComparator} is null
     */
    public VersionComparator(Comparator<String> partComparator) {
        if (partComparator == null)
            throw new IllegalArgumentException("null partComparator");
        this.partComparator = partComparator;
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to <code>VersionComparator(new VersionPartComparator())</code>.
     */
    public VersionComparator() {
        this(new VersionPartComparator());
    }

    @Override
    public int compare(String v1, String v2) {

        // Chop up version into parts, where a part is a contiguous all-digit or all-non-digit sequence
        List<List<String>> partsList = new ArrayList<List<String>>(2);
        partsList.add(this.separateIntoParts(v1));
        partsList.add(this.separateIntoParts(v2));

        // Compare parts pair-wise
        for (int i = 0; true; i++) {
            final boolean hasPart1 = i < partsList.get(0).size();
            final boolean hasPart2 = i < partsList.get(1).size();
            if (!hasPart1 && !hasPart2)
                return 0;
            else if (hasPart1 && !hasPart2)
                return 1;
            else if (!hasPart1 && hasPart2)
                return -1;
            final String part1 = partsList.get(0).get(i);
            final String part2 = partsList.get(1).get(i);
            int diff = this.partComparator.compare(part1, part2);
            if (diff != 0)
                return diff;
        }
    }

    /**
     * Split a version string into parts.
     *
     * <p>
     * The implementation in {@link VersionComparator} defines a <b>part</b> as a contiguous sequence
     * of one or more digits, or a contiguous sequence of one or more non-digits, and where the period character
     * ('.') serves as a part separator.
     * </p>
     *
     * <p>
     * Subclasses may choose to override this method to define parts differently.
     * </p>
     *
     * @param version the original version string
     * @return the parts of the version string
     */
    protected List<String> separateIntoParts(String version) {
        final ArrayList<String> partsList = new ArrayList<String>();
        int pos = 0;
        for (Matcher matcher = PART_PATTERN.matcher(version); matcher.find(pos); pos = matcher.end()) {
            final String part = matcher.group();
            if (!part.equals("."))
                partsList.add(part);
            else if (matcher.end() == version.length() || version.charAt(matcher.end()) == '.')
                partsList.add("");
        }
        return partsList;
    }
}

