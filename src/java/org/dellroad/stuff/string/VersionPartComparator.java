
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.math.BigInteger;
import java.util.Comparator;

/**
 * Comparator for version number parts.
 *
 * <p>
 * This comparator puts numerical parts before non-numerical parts.
 * Then, numerical parts are compared numerically, and non-numerical parts are compared lexically.
 * </p>
 *
 * @see VersionComparator
 */
public class VersionPartComparator implements Comparator<String> {

    @Override
    public int compare(String part1, String part2) {
        BigInteger num1 = part1.matches("[0-9]+") ? new BigInteger(part1, 10) : null;
        BigInteger num2 = part2.matches("[0-9]+") ? new BigInteger(part2, 10) : null;
        if (num1 != null && num2 != null)
            return num1.compareTo(num2);
        else if (num1 != null)
            return 1;
        else if (num2 != null)
            return -1;
        else
            return part1.compareTo(part2);
    }
}

