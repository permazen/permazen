
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.graph;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.dellroad.stuff.TestSupport;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TopologicalSorterTest extends TestSupport {

    public static final Comparator<String> FORWARD_COMPARATOR = new Comparator<String>() {
        public int compare(String s1, String s2) {
            return s1.compareTo(s2);
        }
    };
    public static final Comparator<String> BACKWARD_COMPARATOR = Collections.reverseOrder(FORWARD_COMPARATOR);

    @Test(dataProvider = "data")
    public void testSort(String setup, Boolean forward, String result, String reversedResult) {

        // Create sorter
        TopologicalSorter<String> sorter = new TopologicalSorter<String>(getNodes(setup), getEdgeLister(setup),
          forward == null ? null : forward.booleanValue() ? FORWARD_COMPARATOR : BACKWARD_COMPARATOR);

        // Test sort using normal edges
        List<String> list;
        try {
            list = sorter.sort();
            assertNotNull(result);
            assertEquals(concat(list), result);
        } catch (IllegalArgumentException e) {
            assertNull(result);
        }

        // Test sort using reversed edges
        try {
            list = sorter.sortEdgesReversed();
            assertNotNull(reversedResult);
            assertEquals(concat(list), reversedResult);
        } catch (IllegalArgumentException e) {
            assertNull(reversedResult);
        }
    }

    @DataProvider(name = "data")
    public Object[][] genTestCases() {
        return new Object[][] {

            new Object[] { "a>b,a>c", Boolean.TRUE, "abc", "bca" },
            new Object[] { "a>b,a>c", Boolean.FALSE, "acb", "cba" },
            new Object[] { "a>b,a>c", null, "abc", "bca" },
            new Object[] { "w>q,w>z", null, "wqz", "qzw" },

            new Object[] { "f,q,z,a>b,a>c", Boolean.TRUE, "abcfqz", "bcafqz" },
            new Object[] { "f,q,z,a>b,a>c", Boolean.FALSE, "zqfacb", "zqfcba" },
            new Object[] { "f,q,z,a>b,a>c", null, "fqzabc", "fqzbca" },

            new Object[] { "f,q,z>a,a>b,a>c", Boolean.TRUE, "fqzabc", "bcafqz" },
            new Object[] { "f,q,z>a,a>b,a>c", Boolean.FALSE, "zqfacb", "cbazqf" },
            new Object[] { "f,q,z>a,a>b,a>c", null, "fqzabc", "fqbcaz" },
            new Object[] { "z,y,x>a,a>b,a>c", null, "zyxabc", "zybcax" },

            new Object[] { "a,b,c,d,e", Boolean.TRUE, "abcde", "abcde" },
            new Object[] { "e,d,c,b,a", Boolean.TRUE, "abcde", "abcde" },
            new Object[] { "a,b,c,d,e", Boolean.FALSE, "edcba", "edcba" },
            new Object[] { "e,d,c,b,a", Boolean.FALSE, "edcba", "edcba" },
            new Object[] { "a,b,c,d,e", null, "abcde", "abcde" },
            new Object[] { "e,d,c,b,a", null, "edcba", "edcba" },

            new Object[] { "a>b,b>c,c>a", null, null, null },
            new Object[] { "a>a", null, null, null },
            new Object[] { "a>a,b", null, null, null },

            new Object[] { "a>d,a>e,a>f,b>d,b>e,b>f,c>d,c>e,c>f", Boolean.TRUE, "abcdef", "defabc" },
            new Object[] { "a>d,a>e,a>f,b>d,b>e,b>f,c>d,c>e,c>f", Boolean.FALSE, "cbafed", "fedcba" },
            new Object[] { "a>d,a>e,a>f,b>d,b>e,b>f,c>d,c>e,c>f", null, "abcdef", "defabc" },

            new Object[] { "a>b,b>c,c>d,d>e,e>f", Boolean.TRUE, "abcdef", "fedcba" },
            new Object[] { "a>b,b>c,c>d,d>e,e>f", Boolean.FALSE, "abcdef", "fedcba" },
            new Object[] { "a>b,b>c,c>d,d>e,e>f", null, "abcdef", "fedcba" },

            new Object[] { "a>e,e>c,c>f,f>b,b>d", Boolean.TRUE, "aecfbd", "dbfcea" },
            new Object[] { "a>e,e>c,c>f,f>b,b>d", Boolean.FALSE, "aecfbd", "dbfcea" },
            new Object[] { "a>e,e>c,c>f,f>b,b>d", null, "aecfbd", "dbfcea" },
        };
    }

    public static Set<String> getNodes(String setup) {
        LinkedHashSet<String> nodes = new LinkedHashSet<String>();
        for (String relation : setup.split(",")) {
            switch (relation.length()) {
            case 1:
                nodes.add(relation);
                break;
            case 3:
                assert relation.charAt(1) == '>';
                nodes.add(relation.substring(0, 1));
                nodes.add(relation.substring(2));
                break;
            default:
                assert false;
                break;
            }
        }
        return nodes;
    }

    public static String concat(List<String> list) {
        StringBuilder buf = new StringBuilder();
        for (String elem : list)
            buf.append(elem);
        return buf.toString();
    }

    public static TopologicalSorter.EdgeLister<String> getEdgeLister(String setup) {

        // Build edge map
        final HashMap<String, HashSet<String>> edgeMap = new HashMap<String, HashSet<String>>();
        for (String relation : setup.split(",")) {
            if (relation.length() != 3)
                continue;
            assert relation.charAt(1) == '>';
            String from = relation.substring(0, 1);
            String to = relation.substring(2);
            HashSet<String> targets = edgeMap.get(from);
            if (targets == null) {
                targets = new HashSet<String>();
                edgeMap.put(from, targets);
            }
            targets.add(to);
        }

        // Return lister
        return new TopologicalSorter.EdgeLister<String>() {
            public Set<String> getOutEdges(String node) {
                assert node.length() == 1;
                HashSet<String> set = edgeMap.get(node);
                return set != null ? set : Collections.<String>emptySet();
            }
        };
    }
}

