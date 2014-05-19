
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsimpledb.core.Database;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SchemaItem;
import org.jsimpledb.core.SchemaVersion;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.NameIndex;
import org.jsimpledb.schema.SchemaObject;
import org.jsimpledb.util.NavigableSets;
import org.jsimpledb.util.ParseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Superclass of all CLI commands.
 */
public abstract class Command implements Parser<Action> {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());
    protected final String name;
    protected final ParamParser paramParser;

// Constructors

    protected Command(String spec) {
        if (spec == null)
            throw new IllegalArgumentException("null spec");
        final Matcher matcher = Pattern.compile("([^\\s]+)(\\s+(.*))?").matcher(spec);
        if (!matcher.matches())
            throw new IllegalArgumentException("invalid command specification `" + spec + "'");
        this.name = matcher.group(1);
        final String paramSpec = matcher.group(3);
        this.paramParser = new ParamParser(paramSpec != null ? paramSpec : "") {
            @Override
            protected Parser<?> getParser(String typeName) {
                try {
                    return super.getParser(typeName);
                } catch (IllegalArgumentException e) {
                    return Command.this.getParser(typeName);
                }
            }
        };
    }

// Command stuff

    public String getName() {
        return this.name;
    }

    public String getUsage() {
        return this.paramParser.getUsage(this.name);
    }

    public abstract String getHelpSummary();

    public String getHelpDetail() {
        return this.getHelpSummary();
    }

    /**
     * Parse command line and return command action.
     *
     * <p>
     * The implementation in {@link ParamParser} parses the parameters and delegates to {@link #getAction} with the result.
     * </p>
     *
     * @throws ParseException if parameters are invalid
     */
    @Override
    public Action parse(Session session, ParseContext ctx, boolean complete) {
        return this.getAction(session, ctx, complete, this.paramParser.parse(session, ctx, complete));
    }

    /**
     * Process command line parameters and return action.
     *
     * @throws ParseException if parameters are invalid
     */
    public abstract Action getAction(Session session, ParseContext ctx, boolean complete, Map<String, Object> params);

// Channel access

    public Channel<?> get(Session session, int depth) {
        this.checkDepth(session, depth + 1);
        return Iterables.get(session.getStack(), depth);
    }

    public <T> Channel<? extends T> get(Session session, int depth, Class<T> type) {
        return this.get(session, depth, TypeToken.of(type));
    }

    public <T> Channel<? extends T> get(Session session, int depth, TypeToken<T> typeToken) {
        return this.cast(this.get(session, depth), typeToken);
    }

    public Channel<?> pop(Session session) {
        this.checkDepth(session, 1);
        return session.getStack().pop();
    }

    public <T> Channel<? extends T> pop(Session session, Class<T> type) {
        return this.pop(session, TypeToken.of(type));
    }

    public <T> Channel<? extends T> pop(Session session, TypeToken<T> typeToken) {
        return this.cast(this.pop(session), typeToken);
    }

    public Channel<?> remove(Session session, int depth) {
        this.checkDepth(session, depth + 1);
        final Iterator<Channel<?>> i = session.getStack().iterator();
        Iterators.advance(i, depth);
        Channel<?> channel = i.next();
        i.remove();
        return channel;
    }

    public <T> Channel<? extends T> remove(Session session, int depth, Class<T> type) {
        return this.remove(session, depth, TypeToken.of(type));
    }

    public <T> Channel<? extends T> remove(Session session, int depth, TypeToken<T> typeToken) {
        return this.cast(this.remove(session, depth), typeToken);
    }

    public <T> Channel<? extends T> cast(Channel<?> channel, Class<T> type) {
        return this.cast(channel, TypeToken.of(type));
    }

    public void push(Session session, Channel<?> channel) {
        session.getStack().push(channel);
    }

    public <T> void push(Session session, Class<T> type, final T value) {
        this.push(session, new AbstractChannel<T>(type) {
            @Override
            public Set<T> getItems(Session session) {
                return Collections.<T>singleton(value);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public <T> Channel<? extends T> cast(Channel<?> channel, TypeToken<T> typeToken) {
        final TypeToken<?> itemType = channel.getItemType().getTypeToken();
        if (!typeToken.isAssignableFrom(itemType)) {
            throw new IllegalArgumentException("the `" + this.getName() + "' command expects a channel of type "
              + typeToken + " but the channel found has type " + itemType);
        }
        return (Channel<? extends T>)channel;
    }

    protected void checkDepth(Session session, int min) {
        final int depth = session.getStack().size();
        if (depth < min) {
            throw new IllegalArgumentException("the `" + this.getName() + "' command expects at least " + min
              + " input channel" + (min != 1 ? "s" : "") + ", but there are only "
              + depth + " channel" + (depth != 1 ? "s" : "") + " on the stack");
        }
    }

// Parsing

    /**
     * Convert parameter spec type name into a {@link Parser}. Used for custom type names not supported by {@link ParamParser}.
     *
     * <p>
     * The implementation in {@link ParamParser} supports all {@link org.jsimpledb.core.FieldType}s registered with the database,
     * {@code type} for an object type name (returns {@link Integer}), and {@code objid} for an object ID (returns {@link ObjId}).
     * </p>
     */
    protected Parser<?> getParser(String typeName) {
        if (typeName.equals("type"))
            return new TypeNameParser();
        if (typeName.equals("objid"))
            return new ObjIdParser();
        return FieldTypeParser.getFieldTypeParser(typeName);
    }

// TypeNameParser

    /**
     * Parses an object type name.
     */
    private class TypeNameParser implements Parser<ObjType> {

        @Override
        public ObjType parse(Session session, ParseContext ctx, boolean complete) {

            // Try to parse as an integer
            final Transaction tx = session.getTransaction();
            final Database db = session.getDatabase();
            try {
                final int storageId = db.getFieldTypeRegistry().getFieldType(TypeToken.of(Integer.TYPE)).fromString(ctx);
                return tx.getSchemaVersion().getSchemaItem(storageId, ObjType.class);
            } catch (IllegalArgumentException e) {
                // ignore
            }

            // Try to parse as an object type name with optional #version suffix
            final Matcher matcher;
            try {
                matcher = ctx.matchPrefix("([^\\s;#]+)(#([0-9]+))?");
            } catch (IllegalArgumentException e) {
                throw new ParseException(ctx, "invalid object type `" + Util.truncate(ctx.getInput(), 16)
                  + "' given to the `" + Command.this.getName() + "' command");
            }
            final String typeName = matcher.group(1);
            final String versionString = matcher.group(3);

            // Get name index
            final NameIndex nameIndex;
            if (versionString != null) {
                final int version;
                try {
                    nameIndex = new NameIndex(tx.getSchema().getVersion(Integer.parseInt(versionString)).getSchemaModel());
                } catch (IllegalArgumentException e) {
                    throw new ParseException(ctx, "invalid object type schema version `" + versionString
                      + "' given to the `" + Command.this.getName() + "' command");
                }
            } else
                nameIndex = session.getNameIndex();

            // Find type by name
            final Set<SchemaObject> schemaObjects = nameIndex.getSchemaObjects(typeName);
            switch (schemaObjects.size()) {
            case 0:
                throw new ParseException(ctx, "unknown object type `" + typeName + "' given to the `"
                  + Command.this.getName() + "' command")
                   .addCompletions(Util.complete(nameIndex.getSchemaObjectNames(), typeName));
            case 1:
                return tx.getSchemaVersion().getSchemaItem(schemaObjects.iterator().next().getStorageId(), ObjType.class);
            default:
                throw new ParseException(ctx, "ambiguous object type `" + typeName + "' given to the `"
                  + Command.this.getName() + "' command: there are multiple matching object types"
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
    private class ObjIdParser implements Parser<ObjId> {

        @Override
        public ObjId parse(Session session, ParseContext ctx, boolean complete) {

            // Get parameter
            final Matcher matcher = ctx.tryPattern("([0-9A-Fa-f]{0,16})");
            if (matcher == null)
                throw new ParseException(ctx, "Usage: " + Command.this.getUsage());
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
                    throw new ParseException(ctx, "Usage: " + Command.this.getUsage());
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
                            if ((min != null && storageId < min.getStorageId())
                              || (max != null && storageId > max.getStorageId()))
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
            throw new ParseException(ctx, "Usage: " + Command.this.getUsage())
              .addCompletions(Util.complete(completions, param));
        }
    }
}

