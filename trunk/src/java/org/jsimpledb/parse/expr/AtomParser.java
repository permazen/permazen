
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import com.google.common.reflect.TypeToken;

import java.util.Date;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dellroad.stuff.java.Primitive;
import org.dellroad.stuff.string.StringEncoder;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.parse.ObjIdParser;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;

/**
 * The lowest parse level.
 */
public class AtomParser implements Parser<Node> {

    public static final AtomParser INSTANCE = new AtomParser();

    private final SpaceParser spaceParser = new SpaceParser();
    private final TreeSet<String> identifierCompletions;

    public AtomParser() {
        this(null);
    }

    /**
     * Constructor.
     *
     * @param identifierCompletions set of valid identifiers tab completions, or null to allow any identifiers
     */
    public AtomParser(Iterable<String> identifierCompletions) {
        if (identifierCompletions != null) {
            this.identifierCompletions = new TreeSet<String>();
            for (String identifierCompletion : identifierCompletions)
                this.identifierCompletions.add(identifierCompletion);
        } else
            this.identifierCompletions = null;
    }

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {
        final int mark = ctx.getIndex();

        // Check for parenthesized expression
        if (ctx.tryLiteral("(")) {
            this.spaceParser.parse(ctx, complete);
            final Node node = AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
            this.spaceParser.parse(ctx, complete);
            if (!ctx.tryLiteral(")"))
                throw new ParseException(ctx).addCompletion(") ");
            return node;
        }

        // Try to match null
        if (this.tryWord(ctx, "null") != null)
            return new LiteralNode(null);

        // Try to match boolean
        final String booleanMatch = this.tryWord(ctx, "false|true");
        if (booleanMatch != null)
            return new LiteralNode(Boolean.valueOf(booleanMatch));

        // Try to match double, float, int, or long literal
        final int start = ctx.getIndex();
        final String floatMatch = this.tryWord(ctx, Primitive.FLOAT.getParsePattern() + "[fF]");
        if (floatMatch != null) {
            try {
                return new LiteralNode(Primitive.FLOAT.parseValue(floatMatch.substring(0, floatMatch.length() - 1)));
            } catch (IllegalArgumentException e) {
                ctx.setIndex(start);
            }
        }
        final String longMatch = this.tryWord(ctx, Primitive.LONG.getParsePattern() + "[Ll]");
        if (longMatch != null) {
            try {
                return new LiteralNode(Primitive.LONG.parseValue(longMatch.substring(0, longMatch.length() - 1)));
            } catch (IllegalArgumentException e) {
                ctx.setIndex(start);
            }
        }
        final String intMatch = this.tryFollow(ctx, Primitive.INTEGER.getParsePattern(), "[^.\\p{javaJavaIdentifierPart}]");
        if (intMatch != null) {
            try {
                return new LiteralNode(Primitive.INTEGER.parseValue(intMatch));
            } catch (IllegalArgumentException e) {
                ctx.setIndex(start);
            }
        }
        final String doubleMatch = this.tryFollow(ctx, Primitive.DOUBLE.getParsePattern(), "[^.\\p{javaJavaIdentifierPart}]");
        if (doubleMatch != null) {
            try {
                return new LiteralNode(Primitive.DOUBLE.parseValue(doubleMatch));
            } catch (IllegalArgumentException e) {
                ctx.setIndex(start);
            }
        }

        // Try to match a char literal
        String match = this.tryWord(ctx,
          StringEncoder.ENQUOTE_PATTERN.toString().replaceAll("\"", "'").replaceAll("\\*", ""));    // kludge
        if (match != null) {
            match = match.substring(1, match.length() - 1);
            if (match.length() > 0 && match.charAt(0) == '\'') {
                ctx.setIndex(start);
                throw new ParseException(ctx, "invalid character: contains unescaped single quote");
            }
            match = StringEncoder.decode(match.replaceAll(Pattern.quote("\\'"), Matcher.quoteReplacement("'")));
            if (match.length() != 1) {
                ctx.setIndex(start);
                throw new ParseException(ctx, "invalid character: quotes must contain exactly one character");
            }
            return new LiteralNode(match.charAt(0));
        }

        // Try to match a string literal
        final Matcher stringMatch = ctx.tryPattern(StringEncoder.ENQUOTE_PATTERN);
        if (stringMatch != null)
            return new LiteralNode(new String(StringEncoder.dequote(stringMatch.group())));

        // Try to match some types from type registry within square brackets
        final FieldTypeRegistry registry = session.getDatabase().getFieldTypeRegistry();
        for (Class<?> type : new Class<?>[] { String.class, Date.class }) {
            final FieldType<?> fieldType = registry.getFieldType(TypeToken.of(type));
            try {
                ctx.expect('[');
                final LiteralNode node = new LiteralNode(fieldType.fromParseableString(ctx));
                ctx.expect(']');
                return node;
            } catch (IllegalArgumentException e) {
                ctx.setIndex(mark);
                continue;
            }
        }

        // Try to match variable
        if (ctx.tryLiteral("$")) {
            final Matcher varMatcher = ctx.tryPattern(VarNode.NAME_PATTERN);
            if (varMatcher == null)
                throw new ParseException(ctx).addCompletions(session.getVars().keySet());
            final String name = varMatcher.group();
            if (ctx.isEOF() && complete)
                throw new ParseException(ctx).addCompletions(ParseUtil.complete(session.getVars().keySet(), name));
            return new VarNode(name);
        }

        // Try to match object literal
        if (ctx.tryLiteral("@")) {
            final ObjId id = new ObjIdParser().parse(session, ctx, complete);
            return !session.hasJSimpleDB() ? new LiteralNode(id) : new Node() {
                @Override
                public Value evaluate(ParseSession session) {
                    return new Value(JTransaction.getCurrent().getJObject(id));
                }
            };
        }

        // Try to match identifier; support tab-completion of configured identifiers, if any
        final Matcher identMatcher = ctx.tryPattern(IdentNode.NAME_PATTERN);
        if (identMatcher != null) {
            final String name = identMatcher.group();
            if (complete && ctx.isEOF() && this.identifierCompletions != null && !name.equals("new"))
                throw new ParseException(ctx).addCompletions(ParseUtil.complete(this.identifierCompletions, name));
            return new IdentNode(name);
        }
        if (complete && ctx.isEOF() && this.identifierCompletions != null)
            throw new ParseException(ctx).addCompletions(this.identifierCompletions);

        // No match
        throw new ParseException(ctx);
    }

// Match a pattern not followed by a identifier letter

    private String tryWord(ParseContext ctx, Pattern pattern) {
        return this.tryWord(ctx, pattern.toString());
    }

    private String tryWord(ParseContext ctx, String pattern) {
        return this.tryFollow(ctx, pattern, "[^\\p{javaJavaIdentifierPart}]");
    }

    private String tryFollow(ParseContext ctx, Pattern pattern, String follow) {
        return this.tryFollow(ctx, pattern.toString(), follow);
    }

    private String tryFollow(ParseContext ctx, String pattern, String follow) {
        final Matcher matcher = Pattern.compile("(" + pattern + ")(" + follow + ".*)?").matcher(ctx.getInput());
        if (!matcher.matches())
            return null;
        final String match = matcher.group(1);
        ctx.setIndex(ctx.getIndex() + match.length());
        return match;
    }
}

