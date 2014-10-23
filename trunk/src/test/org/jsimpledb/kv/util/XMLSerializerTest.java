
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.kv.util;

import com.google.common.base.Converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.NavigableMap;

import org.jsimpledb.TestSupport;
import org.jsimpledb.util.ByteUtil;
import org.jsimpledb.util.ConvertedNavigableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

public class XMLSerializerTest extends TestSupport {

    @Test
    public void testXMLSerializer() throws Exception {

        final NavigableMap<byte[], byte[]> data1 = new NavigableMapKVStore().getNavigableMap();

        data1.put(b("8901"), b(""));
        data1.put(b("0123"), b("4567"));
        data1.put(b("33"), b("44444444"));
        data1.put(b("22"), b(""));

        final String expectedIndent =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
          + "<entries>\n"
          + "    <entry>\n"
          + "        <key>0123</key>\n"
          + "        <value>4567</value>\n"
          + "    </entry>\n"
          + "    <entry>\n"
          + "        <key>22</key>\n"
          + "    </entry>\n"
          + "    <entry>\n"
          + "        <key>33</key>\n"
          + "        <value>44444444</value>\n"
          + "    </entry>\n"
          + "    <entry>\n"
          + "        <key>8901</key>\n"
          + "    </entry>\n"
          + "</entries>";
        final String expectedNodent = expectedIndent.replaceAll(">\\s+", ">");

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        new XMLSerializer(new NavigableMapKVStore(data1)).write(buf, true);
        final byte[] xmlIndent = buf.toByteArray();
        buf.reset();
        new XMLSerializer(new NavigableMapKVStore(data1)).write(buf, false);
        final byte[] xmlNodent = buf.toByteArray();

        final String actualIndent = new String(xmlIndent);
        final String actualNodent = new String(xmlNodent);

        Assert.assertEquals(actualIndent, expectedIndent);
        Assert.assertEquals(actualNodent, expectedNodent);

        final NavigableMap<byte[], byte[]> data2 = new NavigableMapKVStore().getNavigableMap();
        new XMLSerializer(new NavigableMapKVStore(data2)).read(new ByteArrayInputStream(xmlIndent));
        Assert.assertEquals(s(data1), s(data2));

        data2.clear();
        new XMLSerializer(new NavigableMapKVStore(data2)).read(new ByteArrayInputStream(xmlNodent));
        Assert.assertEquals(s(data1), s(data2));
    }

    private static NavigableMap<String, String> s(NavigableMap<byte[], byte[]> map) {
        final Converter<String, byte[]> converter = ByteUtil.STRING_CONVERTER.reverse();
        return new ConvertedNavigableMap<String, String, byte[], byte[]>(map, converter, converter);
    }
}

