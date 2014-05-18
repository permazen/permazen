
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.core.FieldType;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;

/**
 * Parses a command with optional flags.
 *
 * <p>
 * Spec syntax examples:
 * <ul>
 *  <li><code>-v</code> - boolean flag</li>
 *  <li><code>-v:</code> - string flag</li>
 *  <li><code>-v:int</code> - integer flag</li>
 *  <li><code>param</code> - string parameter</li>
 *  <li><code>param:int</code> - integer parameter</li>
 *  <li><code>param:objid</code> - object ID</li>
 *  <li><code>param:type</code> - object type name converted to storage ID</li>
 *  <li><code>param?</code> - optional parameter (last param(s) only)</li>
 *  <li><code>param*</code> - array of zero or more parameters (last param only)</li>
 *  <li><code>param+</code> - array of one or more parameters (last param only)</li>
 *  <li><code>param:int+</code> - array of one or more int parameters (last param only)</li>
 * </ul>
 * </p>
 */
public class ParamParser {

    private final Command command;
    private final FieldTypeRegistry registry;
    private final HashMap<String, Parser<?>> optionParsers = new HashMap<>();
    private final LinkedHashMap<String, Parser<?>> paramParsers = new LinkedHashMap<>();
    private final HashMap<String, Integer> mins = new HashMap<>();
    private final HashMap<String, Integer> maxs = new HashMap<>();
    private final boolean paramRequired;

    public ParamParser(Command command) {
        this(command, "");
    }

    public ParamParser(Command command, String specs) {
        this(new FieldTypeRegistry(), command, specs);
    }

    public ParamParser(FieldTypeRegistry registry, Command command, String specs) {
        if (registry == null)
            throw new IllegalArgumentException("null registry");
        if (command == null)
            throw new IllegalArgumentException("null command");
        if (specs == null)
            throw new IllegalArgumentException("null specs");
        this.registry = registry;
        this.command = command;
        if (specs.length() == 0) {
            this.paramRequired = false;
            return;
        }
        boolean anyParamRequired = false;
        for (String spec : specs.split("\\s+")) {

            // Get cardinality
            final int min;
            final int max;
            if (spec.endsWith("?")) {
                min = 0;
                max = 1;
                spec = spec.substring(0, spec.length() - 1);
            } else if (spec.endsWith("*")) {
                min = 0;
                max = Integer.MAX_VALUE;
                spec = spec.substring(0, spec.length() - 1);
            } else if (spec.endsWith("+")) {
                min = 1;
                max = Integer.MAX_VALUE;
                spec = spec.substring(0, spec.length() - 1);
            } else {
                min = 1;
                max = 1;
            }

            // Get parser for parameter/option
            final int colon = spec.indexOf(':');
            final Parser<?> parser;
            if (colon != -1) {
                parser = this.getParser(spec.substring(colon + 1));
                spec = spec.substring(0, colon);
            } else if (!spec.startsWith("-"))
                parser = new WordParser();
            else
                parser = null;
            if (spec.startsWith("-"))
                this.optionParsers.put(spec, parser);
            else {
                this.paramParsers.put(spec, parser);
                this.mins.put(spec, min);
                this.maxs.put(spec, max);
                if (min > 0)
                    anyParamRequired = true;
            }
        }
        this.paramRequired = anyParamRequired;
    }

    protected Parser<?> getParser(String typeName) {
        if (typeName.length() == 0)
            return new WordParser();
        if (typeName.equals("type"))
            return new TypeNameParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        return this.createFieldTypeParser(this.registry.getFieldType(typeName));
    }

    // This method exists solely to bind the generic type parameters
    protected <T> FieldTypeParser<T> createFieldTypeParser(FieldType<T> fieldType) {
        return new FieldTypeParser<T>(fieldType);
    }

    /**
     * Parse a command line.
     *
     * @param session associated session
     * @param ctx parse context positioned at whitespace preceeding parameters (if any)
     * @param complete true if we're only determining completions
     * @throws ParseException if parse fails
     */
    public Map<String, Object> parseParameters(Session session, ParseContext ctx, boolean complete) {

        // Store results here
        final HashMap<String, Object> values = new HashMap<String, Object>();

        // First parse options
        boolean needSpace = this.paramRequired;
        while (true) {
            final HashSet<String> completions = new HashSet<>();
            new SpaceParser(needSpace).parse(ctx);
            needSpace = false;
            if (ctx.getInput().matches("^--([\\s;].*)?$")) {
                ctx.setIndex(ctx.getIndex() + 2);
                needSpace = this.paramRequired;
                break;
            }
            final Matcher matcher = ctx.tryPattern("(-[^\\s;]+)");
            if (matcher == null)
                break;
            final String option = matcher.group(1);
            if (!this.optionParsers.containsKey(option)) {
                throw new ParseException(ctx, "unrecognized option `" + option + "' given to the `" + this.command.getName()
                  + "' command").addCompletions(Util.complete(this.optionParsers.keySet(), option));
            }
            final Parser<?> parser = this.optionParsers.get(option);
            if (parser != null) {
                new SpaceParser(needSpace).parse(ctx);
                needSpace = false;
            }
            values.put(option, parser != null ? parser.parse(session, ctx, complete) : null);
            needSpace = this.paramRequired;
        }

        // Next parse parameters
        for (Map.Entry<String, Parser<?>> entry : this.paramParsers.entrySet()) {
            final String param = entry.getKey();
            final Parser<?> parser = entry.getValue();
            final int min = this.mins.get(param);
            final int max = this.maxs.get(param);
            final ArrayList<Object> paramValues = new ArrayList<>();
            while (paramValues.size() < max) {
                new SpaceParser(needSpace).parse(ctx);
                needSpace = false;
                if (!ctx.getInput().matches("^[^\\s;].*$")) {
                    if (complete) {
                        parser.parse(session, new ParseContext(""), true);
                        throw new ParseException(ctx, "");      // should never get here
                    }
                    break;
                }
                paramValues.add(parser.parse(session, ctx, complete));
                needSpace = paramValues.size() < min;
            }
            if (paramValues.size() < min) {
                final ParseException e = new ParseException(ctx,
                  "missing `" + param + "' parameter to the `" + ParamParser.this.command.getName() + "' command");
                if (complete) {
                    try {
                        parser.parse(session, new ParseContext(""), true);
                    } catch (ParseException e2) {
                        e.addCompletions(e2.getCompletions());
                    }
                }
                throw e;
            }
            if (max > 1)
                values.put(param, Arrays.asList(paramValues.toArray()));
            else if (!paramValues.isEmpty())
                values.put(param, paramValues.get(0));
        }

        // Check for trailing garbage
        new SpaceParser().parse(ctx);
        if (!ctx.getInput().matches("^(;.*)?$"))
            throw new ParseException(ctx, "Usage: " + ParamParser.this.command.getUsage());

        // Done
        return values;
    }

    public static String truncate(String string, int len) {
        if (len < 4)
            throw new IllegalArgumentException("len = " + len + " < 4");
        if (string.length() <= len)
            return string;
        return string.substring(0, len - 3) + "...";
    }

// Parser

    public interface Parser<T> {

        T parse(Session session, ParseContext ctx, boolean complete);
    }

// FieldTypeParser

    /**
     * Parses a value associated with a {@link FieldType}.
     */
    public class FieldTypeParser<T> implements Parser<T> {

        private final FieldType<T> fieldType;

        public FieldTypeParser(Class<T> type) {
            this(TypeToken.of(type));
        }

        public FieldTypeParser(TypeToken<T> typeToken) {
            this(ParamParser.this.registry.getFieldType(typeToken));
        }

        public FieldTypeParser(FieldType<T> fieldType) {
            if (fieldType == null)
                throw new IllegalArgumentException("null fieldType");
            this.fieldType = fieldType;
        }

        @Override
        public T parse(Session session, ParseContext ctx, boolean complete) {
            final int start = ctx.getIndex();
            try {
                return this.fieldType.fromString(ctx);
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid " + this.fieldType.getName() + " parameter `"
                  + ParamParser.truncate(ctx.getOriginalInput().substring(start), 16)
                  + "' given to the `" + ParamParser.this.command.getName() + "' command");
            }
        }
    }

// WordParser

    /**
     * Parses a word (i.e., one or more non-whitespace characters).
     */
    public class WordParser implements Parser<String> {

        @Override
        public String parse(Session session, ParseContext ctx, boolean complete) {
            try {
                return ctx.matchPrefix("([^\\s;]+)").group(1);
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "missing parameter to the `" + ParamParser.this.command.getName() + "' command");
            }
        }
    }

// TypeNameParser

    /**
     * Parses an object type name.
     */
    public class TypeNameParser implements Parser<Integer> {

        @Override
        public Integer parse(Session session, ParseContext ctx, boolean complete) {

            // Try to parse as an integer
            final FieldTypeParser<Integer> intParser = new FieldTypeParser<Integer>(Integer.TYPE);
            try {
                return intParser.parse(session, ctx, complete);
            } catch (IllegalArgumentException e) {
                // ignore
            }

            // Try to parse as an object type name with optional #version suffix
            final Matcher matcher;
            try {
                matcher = ctx.matchPrefix("([^\\s;#]+)(#([0-9]+))?");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid object type `" + ParamParser.truncate(ctx.getInput(), 16)
                  + "' given to the `" + ParamParser.this.command.getName() + "' command");
            }
            final String typeName = matcher.group(1);
            final String versionString = matcher.group(3);

            // Get name index
            final NameIndex nameIndex;
            if (versionString != null) {
                final int version;
                try {
                    nameIndex = new NameIndex(session.getTransaction().getSchema().getVersion(
                      intParser.parse(session, new ParseContext(versionString), complete)).getSchemaModel());
                } catch (IllegalArgumentException e) {
                    throw new ParseException(ctx, "invalid object type schema version `" + versionString
                      + "' given to the `" + ParamParser.this.command.getName() + "' command");
                }
            } else
                nameIndex = session.getNameIndex();

            // Find type by name
            final Set<SchemaObject> schemaObjects = nameIndex.getSchemaObjects(typeName);
            switch (schemaObjects.size()) {
            case 0:
                throw new ParseException(ctx, "unknown object type `" + typeName + "' given to the `"
                  + ParamParser.this.command.getName() + "' command")
                   .addCompletions(Util.complete(nameIndex.getSchemaObjectNames(), typeName));
            case 1:
                return schemaObjects.iterator().next().getStorageId();
            default:
                throw new ParseException(ctx, "ambiguous object type `" + typeName + "' given to the `"
                  + ParamParser.this.command.getName() + "' command: there are multiple matching object types"
                  + " with storage IDs " + Lists.transform(Lists.newArrayList(schemaObjects),
                    new Function<SchemaObject, Integer>() {
                        @Override
                        public Integer apply(SchemaObject schemaObject) {
                            return schemaObject.getStorageId();
                        }
                   }) + "; specify by storage ID");
            }
        }
    }

// ObjIdParser

    /**
     * Parses and object ID.
     */
    public class ObjIdParser implements Parser<ObjId> {

        @Override
        public ObjId parse(Session session, ParseContext ctx, boolean complete) {

            // Get parameter
            final Matcher matcher = ctx.tryPattern("([0-9A-Fa-f]{0,16})");
            if (matcher == null)
                throw new ParseException(ctx, "Usage: " + ParamParser.this.command.getUsage());
            final String param = matcher.group(1);

            // Attempt to parse id
            try {
                return new ObjId(param);
            } catch (IllegalArgumentException e) {
                // parse failed - must be a partial ID
            }

            // Get corresponding min & max ObjId (both inclusive)
            final char[] paramChars = param.toCharArray();
            final char[] idChars = new char[16];
            System.arraycopy(paramChars, 0, idChars, 0, paramChars.length);
            Arrays.fill(idChars, paramChars.length, idChars.length, '0');
            final String minString = new String(idChars);
            ObjId min0;
            try {
                min0 = new ObjId(minString);
            } catch (IllegalArgumentException e) {
                if (!minString.startsWith("00"))
                    throw new ParseException(ctx, "Usage: " + ParamParser.this.command.getUsage());
                min0 = null;
            }
            Arrays.fill(idChars, paramChars.length, idChars.length, 'f');
            ObjId max0;
            try {
                max0 = new ObjId(new String(idChars));
            } catch (IllegalArgumentException e) {
                max0 = null;
            }

            // Find object IDs in the range
            final ArrayList<String> completions = new ArrayList<>();
            final ObjId min = min0;
            final ObjId max = max0;
            session.perform(new Action() {
                @Override
                public void run(Session session) throws Exception {
                    final Transaction tx = session.getTransaction();
                    final TreeSet<Integer> storageIds = new TreeSet<>();
                    final ArrayList<NavigableSet<ObjId>> idSets = new ArrayList<>();
                    for (SchemaVersion schemaVersion : tx.getSchema().getSchemaVersions().values()) {
                        for (SchemaItem schemaItem : schemaVersion.getSchemaItemMap().values()) {
                            if (!(schemaItem instanceof ObjType))
                                continue;
                            final int storageId = schemaItem.getStorageId();
                            if ((min != null && storageId < min.getStorageId()) || (max != null && storageId > max.getStorageId()))
                                continue;
                            NavigableSet<ObjId> idSet = tx.getAll(storageId);
                            if (min != null) {
                                try {
                                    idSet = idSet.tailSet(min, true);
                                } catch (IllegalArgumentException e) {
                                    // ignore
                                }
                            }
                            if (max != null) {
                                try {
                                    idSet = idSet.headSet(max, true);
                                } catch (IllegalArgumentException e) {
                                    // ignore
                                }
                            }
                            idSets.add(idSet);
                        }
                    }
                    int count = 0;
                    for (ObjId id : NavigableSets.union(idSets)) {
                        completions.add(id.toString());
                        count++;
                        if (count >= session.getLineLimit() + 1)
                            break;
                    }
                }
            });

            // Throw exception with completions
            throw new ParseException(ctx, ParamParser.this.command.getUsage()).addCompletions(Util.complete(completions, param));
        }
    }
}

