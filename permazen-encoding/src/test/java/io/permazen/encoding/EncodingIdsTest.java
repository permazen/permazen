
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import io.permazen.test.TestSupport;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class EncodingIdsTest extends TestSupport {

    @Test(dataProvider = "cases")
    public void testAlias(boolean ok, String alias) throws Exception {

        // Go from alias -> EncodingId
        final EncodingId encodingId;
        try {
            encodingId = EncodingIds.idForAlias(alias);
            assert ok : "expected failure but got success";
        } catch (IllegalArgumentException e) {
            assert !ok : "expected success but got " + e;
            return;
        }
        this.log.debug("alias \"{}\" <-> \"{}\"", alias, encodingId);

        // Go from EncodingId -> alias
        final String expected = alias;
        final String actual = EncodingIds.aliasForId(encodingId);
        assert actual.equals(alias) : "\"" + actual + "\" != \"" + expected + "\"";
    }

    @DataProvider(name = "cases")
    public Object[][] genCases() throws Exception {
        return new Object[][] {

            {   true,   "int"                   },
            {   true,   "foo"                   },
            {   true,   "foo[]"                 },
            {   true,   "foo[][][][]",          },
            {   false,  "3bar"                  },
            {   true,   "foo-bar"               },
            {   true,   "foo-_"                 },
            {   false,  "foo-"                  },
            {   false,  "-foo"                  },

            {   true,   "foo"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"  },

            {   false,   "foo"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"
                            + "[][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][][]"  },

        };
    }
}
