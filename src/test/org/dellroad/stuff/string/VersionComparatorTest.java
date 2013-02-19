
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.ArrayList;
import java.util.List;

import org.dellroad.stuff.TestSupport;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class VersionComparatorTest extends TestSupport {

    @Test(dataProvider = "cases")
    public void compareVersions(String v1, String op, String v2) {
        int expected;
        if (op.equals("<"))
            expected = -1;
        else if (op.equals(">"))
            expected = 1;
        else if (op.equals("="))
            expected = 0;
        else
            throw new RuntimeException("bogus op: " + op);
        VersionComparator versionComparator = new VersionComparator();
        int actual = versionComparator.compare(v1, v2);
        Assert.assertEquals((int)Math.signum(actual), expected);
    }

    @DataProvider(name = "cases")
    public String[][] genTestCases() {
        final String[] versions = new String[] {
            "",
            "x",
            "1",
            "=01",
            "1.",
            "1.0",
            "1.0A",
            "1.0A.3",
            "=1.0A3",
            "1.0AA",
            "1.0AA.A",
            "1.0AA.3",
            "1.0B",
            "1.0a",
            "1.0b",
            "1.0.0",
            "=1.0000.0",
            "=0001.0.0",
            "1.0.3",
            "1.0.3.1.1.1.1.1.1.1.1",
            "1.1",
            "1.1.",
            "1.1..",
            "1.1M",
            "2.0",
            "99.0",
        };
        List<String[]> cases = new ArrayList<String[]>();
        boolean[] eq = new boolean[versions.length];
        for (int i = 0; i < versions.length; i++) {
            if (versions[i].length() > 0 && versions[i].charAt(0) == '=') {
                eq[i] = true;
                versions[i] = versions[i].substring(1);
            }
        }
        for (int i = 0; i < versions.length; i++) {
            cases.add(new String[] { versions[i], "=", versions[i] });
            if (i > 0) {
                if (eq[i]) {
                    cases.add(new String[] { versions[i], "=", versions[i - 1] });
                    cases.add(new String[] { versions[i - 1], "=", versions[i] });
                } else {
                    cases.add(new String[] { versions[i], ">", versions[i - 1] });
                    cases.add(new String[] { versions[i - 1], "<", versions[i] });
                }
            }
        }
        return cases.toArray(new String[cases.size()][]);
    }
}

