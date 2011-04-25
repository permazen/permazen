
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dellroad.stuff.TestSupport;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SimpleObjectParserTest extends TestSupport {

    private final SimpleObjectParser<ParseObj> parser = new SimpleObjectParser<ParseObj>(ParseObj.class);

    @Test(dataProvider = "data1")
    public void testParse(String input, String regex, HashMap<Integer, String> patternMap,
      ParseObj expected, Class<? extends Exception> exClass) throws Exception {
        Pattern pattern = Pattern.compile(regex);
        ParseObj actual;
        try {
           actual = parser.parse(input, pattern, patternMap, false);
           assert exClass == null : "expected but didn't get " + exClass.getSimpleName();
        } catch (Exception e) {
            if (exClass == null || !exClass.isInstance(e))
                throw e;
            return;
        }
        assertEquals(actual, expected);
    }

    @Test(dataProvider = "data2")
    public void testNamedGroupParse(String input, String regex, ParseObj expected, Class<? extends Exception> exClass)
      throws Exception {
        ParseObj actual;
        try {
           actual = parser.parse(input, regex, false);
           assert exClass == null : "expected but didn't get " + exClass.getSimpleName();
        } catch (Exception e) {
            if (exClass == null || !exClass.isInstance(e))
                throw e;
            return;
        }
        assertEquals(actual, expected);
    }

    @DataProvider(name = "data1")
    public Object[][] genData1() {
        return new Object[][] {

            // Valid usage
            {
                "b=true i=132 s=\"hello\"",
                "b=(true|false) i=([0-9]+) s=\"([^\"]*)\"",
                map(1, "bval", 2, "ival", 3, "sval"),
                new ParseObj(true, 132, "hello"),
                null
            },

            {
                "b=true i=foo s=\"hello\"",
                "b=(true|false) i=([0-9]+) s=\"([^\"]*)\"",
                map(1, "bval", 2, "ival", 3, "sval"),
                null,
                null
            },

            // Invalid usage
            {
                "b=true i=132 s=\"hello\"",
                "b=(true|false) i=([0-9]+) s=\"[^\"]*\"",
                map(1, "bval", 2, "ival", 3, "sval"),
                null,
                IllegalArgumentException.class
            },

            {
                "b=true i=132 s=\"hello\"",
                "b=(true|false) i=([0-9]+) s=\"([^\"]*)\"",
                map(1, "bval", 2, "ival", 3, "foobar"),
                null,
                IllegalArgumentException.class
            },
        };
    }

    @DataProvider(name = "data2")
    public Object[][] genData2() {
        return new Object[][] {

            // Valid usage
            {
                "b=true i=132 s=\"hello\"",
                "b=({bval}true|false) i=({ival}[0-9]+) s=\"({sval}[^\"]*)\"",
                new ParseObj(true, 132, "hello"),
                null
            },

            {
                "b=true (i=132) aaa s=\"hello\" bbb",
                "b=({bval}true|false) \\(i=({ival}[0-9]+)\\) (a+) s=\"({sval}[^\"]*)\" (b+)",
                new ParseObj(true, 132, "hello"),
                null
            },

            {
                "b=true i=foo s=\"hello\"",
                "b=({bval}true|false) i=({ival}[0-9]+) s=\"({sval}[^\"]*)\"",
                null,
                null
            },

            {
                "b=true",
                "(?s)b=({bval}(true|false))",
                new ParseObj(true, 0, null),
                null
            },

            // Invalid usage
            {
                "",
                "b=({bval",
                null,
                PatternSyntaxException.class
            },

            {
                "b=true ( i=132 s=\"hello\"",
                "b=({bval}true|false) [()] i=({ival}[0-9]+) s=\"({sval}[^\"]*)\"",
                null,
                PatternSyntaxException.class
            },

            {
                "b=true i=132 s=\"hello\"",
                "b=({bval}true|false) i=({ival}[0-9]+) s=\"({foobar}[^\"]*)\"",
                null,
                IllegalArgumentException.class
            },
        };
    }

    private HashMap<Integer, String> map(Object... kv) {
        HashMap<Integer, String> map = new HashMap<Integer, String>(kv.length / 2);
        for (int i = 0; i < kv.length; i += 2)
            map.put((Integer)kv[i], (String)kv[i + 1]);
        return map;
    }

    public static final class ParseObj {

        private boolean bval;
        private int ival;
        private String sval;

        public ParseObj() {
        }

        public ParseObj(boolean bval, int ival, String sval) {
            this.bval = bval;
            this.ival = ival;
            this.sval = sval;
        }

        public boolean getBval() {
            return this.bval;
        }
        public void setBval(boolean bval) {
            this.bval = bval;
        }

        public int getIval() {
            return this.ival;
        }
        public void setIval(int ival) {
            this.ival = ival;
        }

        public String getSval() {
            return this.sval;
        }
        public void setSval(String sval) {
            this.sval = sval;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            ParseObj that = (ParseObj)obj;
            return this.bval == that.bval
              && this.ival == that.ival
              && this.sval == null ? that.sval == null : this.sval.equals(that.sval);
        }

        @Override
        public int hashCode() {
            return (this.bval ? 1 : 0) ^ this.ival ^ (this.sval != null ? this.sval.hashCode() : 0);
        }

        @Override
        public String toString() {
            return "ParseObj[b=" + this.bval + ",i=" + this.ival + ",s=" + this.sval + "]";
        }
    }
}

