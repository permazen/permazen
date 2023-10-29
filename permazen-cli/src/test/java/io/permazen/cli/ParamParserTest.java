
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli;

import io.permazen.cli.parse.EncodingParser;
import io.permazen.cli.parse.Parser;
import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.test.TestSupport;

import java.util.List;
import java.util.Map;

import org.dellroad.jct.core.simple.CommandLineParser;
import org.dellroad.jct.core.simple.SimpleCommandLineParser;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ParamParserTest extends TestSupport {

    private final SimpleCommandLineParser commandLineParser = new SimpleCommandLineParser();

    @Test(dataProvider = "cases")
    public void testParamParser(String specs, String command, Map<String, Object> expected) throws Exception {
        this.log.info("*** ParamParserTest: specs=\"{}\"", specs);
        this.log.info("*** ParamParserTest: command=\"{}\"", command);
        final ParamParser parser = new ParamParser(specs) {
            @Override
            protected Parser<?> getParser(String typeName) {
                final DefaultEncodingRegistry encodingRegistry = new DefaultEncodingRegistry();
                final EncodingId encodingId = encodingRegistry.idForAlias(typeName);
                final Encoding<?> encoding = new DefaultEncodingRegistry().getEncoding(encodingId);
                return encoding != null ? this.createEncodingParser(encoding) : super.getParser(typeName);
            }
            private <T> EncodingParser<T> createEncodingParser(Encoding<T> encoding) {
                return new EncodingParser<>(encoding);
            }
        };
        this.log.info("*** ParamParserTest: optionFlags={}", parser.getOptionFlags());
        this.log.info("*** ParamParserTest: parameters={}", parser.getParameters());
        try {
            final List<String> params = this.commandLineParser.parseCommandLine(command);
            this.log.info("*** ParamParserTest: split command={} (len={})", params, params.size());
            final Map<String, Object> actual = parser.parse(null, params);
            Assert.assertEquals(actual, expected, "\n  ACTUAL: " + actual + "\nEXPECTED: " + expected + "\n");
        } catch (CommandLineParser.SyntaxException | IllegalArgumentException e) {
            if (expected != null)
                throw new Exception("parse failed for \"" + command + "\"", e);
        }
    }

    @DataProvider(name = "cases")
    public Object[][] genParamParserCases() {
        return new Object[][] {

        {
            "",
            "",
            buildMap()
        },

        {
            "-v:verbose",
            "",
            buildMap()
        },

        {
            "-v:verbose",
            " foo",
            null,
        },

        {
            "-v:verbose",
            " -v",
            buildMap("verbose", true)
        },

        {
            "-v:verbose",
            "",
            buildMap()
        },

        {
            "-c:count:int",
            " -v",
            null,
        },

        {
            "-c:count:int",
            " -c",
            null,
        },

        {
            "-c:count:int",
            " -c 234",
            buildMap("count", 234)
        },

        {
            "-c:count:int",
            " -c foo",
            null,
        },

        {
            "-c:count:word",
            " -c foo",
            buildMap("count", "foo")
        },

        {
            "-v:foo:int",
            " -v 123",
            buildMap("foo", 123)
        },

        {
            "foo:int*",
            " 123",
            buildMap("foo", buildList(123)),
        },

        {
            "foo:int*",
            "",
            buildMap("foo", buildList()),
        },

        {
            "-v:version:int -d:debug --foo:foo p1:String p2:double?",
            " --foo -v 123 -d -- \"\\\"hello there\\\"\"",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "hello there")
        },

        {
            "-v:version:int -d:debug --foo:foo p1 p2:double?",
            " --foo -v 123 -d -- \"why not?\" 15.3",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "why not?", "p2", 15.3)
        },

        {
            "-v:version:int -d:debug --foo:foo p1:String p2:double+",
            " --foo -v 123 -d -- \"\\\"frobbely wobbely\\\"\"",
            null
        },

        {
            "-v:version:int -d:debug --foo:foo p1:String p2:double+",
            " --foo -v 123 -d -- \"\\\"goodbye there\\\"\" -342.574e17",
            buildMap("foo", true, "version", 123, "debug", true, "p1", "goodbye there", "p2", buildList(-342.574e17))
        },

        {
            "-v:version:int -d:debug --foo:foo p1:String p2:double*",
            " \\\"\\\" -Infinity -0.0 +0.0 123.45",
            buildMap("p1", "", "p2", buildList(Double.NEGATIVE_INFINITY, -0.0, +0.0, 123.45))
        },

        };
    }
}
