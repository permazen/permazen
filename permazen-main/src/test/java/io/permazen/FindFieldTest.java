
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.JListField;
import io.permazen.annotation.JMapField;
import io.permazen.annotation.JSetField;
import io.permazen.annotation.PermazenType;
import io.permazen.test.TestSupport;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FindFieldTest extends TestSupport {

    private Permazen jdb;

    @BeforeClass
    public void setup() {
        this.jdb = BasicTest.getPermazen(Model1.class);
    }

    @Test(dataProvider = "tests")
    public void testFindField(String fieldName, Boolean expectSubField, Object result) throws Exception {
        final JClass<?> jclass = this.jdb.getJClass(Model1.class);
        final io.permazen.JField jfield;
        try {
            jfield = Util.findField(jclass, fieldName, expectSubField);
        } catch (IllegalArgumentException e) {
            if (result != null)
                throw new AssertionError("expected " + result + " but got " + e, e);
            this.log.debug("got expected " + e);
            return;
        }
        if (jfield == null)
            assert result == null : "expected " + result + " but field not found";
        else
            assert ((Integer)jfield.storageId).equals(result) : "expected " + result + " but got " + jfield;
    }

    @DataProvider(name = "tests")
    public Object[][] genTests() {
        return new Object[][] {

            {   "3x",           null,           null                },
            {   ".string",      null,           null                },
            {   "string.",      null,           null                },
            {   "abc%",         null,           null                },
            {   "aa.bb.cc",     null,           null                },
            {   "map.key.foo",  null,           null                },

            {   "foo",          null,           null                },
            {   "foo",          true,           null                },
            {   "foo",          false,          null                },

            {   "string",       null,           1                   },
            {   "string",       true,           1                   },
            {   "string",       false,          1                   },
            {   "string#1",     null,           1                   },
            {   "string#2",     null,           null                },

            {   "string.foo",   null,           null                },
            {   "string.foo",   true,           null                },
            {   "string.foo",   false,          null                },

            {   "list",         null,           2                   },
            {   "list",         true,           3                   },
            {   "list#2",       true,           3                   },
            {   "list",         false,          2                   },

            {   "list.element", null,           3                   },
            {   "list.element", true,           3                   },
            {   "list.element", false,          null                },

            {   "list.foo",     null,           null                },
            {   "list.foo",     true,           null                },
            {   "list.foo",     false,          null                },

            {   "set",          null,           4                   },
            {   "set",          true,           5                   },
            {   "set#4",        true,           5                   },
            {   "set",          false,          4                   },

            {   "set.element",  null,           5                   },
            {   "set.element",  true,           5                   },
            {   "set.element",  false,          null                },

            {   "set.foo",      null,           null                },
            {   "set.foo",      true,           null                },
            {   "set.foo",      false,          null                },

            {   "map",          null,           6                   },
            {   "map",          true,           null                },
            {   "map#6",        true,           null                },
            {   "map",          false,          6                   },

            {   "map.key",      null,           7                   },
            {   "map.key",      true,           7                   },
            {   "map.key",      false,          null                },
            {   "map#6.key",    null,           7                   },
            {   "map.key#7",    null,           7                   },
            {   "map#6.key#7",  null,           7                   },

            {   "map.value",    null,           8                   },
            {   "map.value",    true,           8                   },
            {   "map.value",    false,          null                },

            {   "map.foo",      null,           null                },
            {   "map.foo",      true,           null                },
            {   "map.foo",      false,          null                },

        };
    }

    @PermazenType
    public interface Model1 extends JObject {

        @JField(storageId = 1)
        String getString();
        void setString(String x);

        @JListField(storageId = 2, element = @JField(storageId = 3))
        List<String> getList();

        @JSetField(storageId = 4, element = @JField(storageId = 5))
        Set<String> getSet();

        @JMapField(storageId = 6, key = @JField(storageId = 7), value = @JField(storageId = 8))
        Map<String, String> getMap();
    }
}
