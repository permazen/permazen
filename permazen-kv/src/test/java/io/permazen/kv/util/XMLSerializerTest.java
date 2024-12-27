
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.util;

import com.google.common.base.Converter;

import io.permazen.test.TestSupport;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.ConvertedNavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.testng.Assert;
import org.testng.annotations.Test;

public class XMLSerializerTest extends TestSupport {

    @Test
    public void testXMLSerializer() throws Exception {

        final ConcurrentSkipListMap<ByteData, ByteData> data1 = new ConcurrentSkipListMap<>();

        data1.put(ByteData.fromHex("8901"), ByteData.empty());
        data1.put(ByteData.fromHex("0123"), ByteData.fromHex("4567"));
        data1.put(ByteData.fromHex("33"), ByteData.fromHex("44444444"));
        data1.put(ByteData.fromHex("22"), ByteData.empty());

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
        new XMLSerializer(new MemoryKVStore(data1)).write(buf, true);
        final byte[] xmlIndent = buf.toByteArray();
        buf.reset();
        new XMLSerializer(new MemoryKVStore(data1)).write(buf, false);
        final byte[] xmlNodent = buf.toByteArray();

        final String actualIndent = new String(xmlIndent);
        final String actualNodent = new String(xmlNodent);

        Assert.assertEquals(actualIndent, expectedIndent);
        Assert.assertEquals(actualNodent, expectedNodent);

        final ConcurrentSkipListMap<ByteData, ByteData> data2 = new ConcurrentSkipListMap<>();
        new XMLSerializer(new MemoryKVStore(data2)).read(new ByteArrayInputStream(xmlIndent));
        Assert.assertEquals(s(data1), s(data2));

        data2.clear();
        new XMLSerializer(new MemoryKVStore(data2)).read(new ByteArrayInputStream(xmlNodent));
        Assert.assertEquals(s(data1), s(data2));
    }

    private static NavigableMap<String, String> s(NavigableMap<ByteData, ByteData> map) {
        final Converter<String, ByteData> converter = ByteUtil.STRING_CONVERTER.reverse();
        return new ConvertedNavigableMap<>(map, converter, converter);
    }
}
