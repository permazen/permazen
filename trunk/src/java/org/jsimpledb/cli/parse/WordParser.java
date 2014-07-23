
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse;

import java.util.Collection;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.cli.Session;
import org.jsimpledb.util.ParseContext;

/**
 * Parses a word (one or more non-whitespace characters).
 */
public class WordParser implements Parser<String> {

    private final Collection<String> words;
    private final String description;

    /**
     * Constructor for when there's a fixed set of possibilities.
     *
     * @param words words to look for
     * @param description what to call words (e.g., "command")
     * @throws IllegalArgumentException if {@code words} is null
     * @throws IllegalArgumentException if {@code description} is null
     */
    public WordParser(Collection<String> words, String description) {
        if (words == null)
            throw new IllegalArgumentException("null words");
        if (description == null)
            throw new IllegalArgumentException("null description");
        this.words = new TreeSet<>(words);
        this.description = description;
    }

    /**
     * Constructor when any word is acceptable.
     *
     * @throws IllegalArgumentException if {@code words} is null
     * @param description what to call words (e.g., "command")
     * @throws IllegalArgumentException if {@code description} is null
     */
    public WordParser(String description) {
        if (description == null)
            throw new IllegalArgumentException("null description");
        this.words = null;
        this.description = description;
    }

    /**
     * Get the set of valid words, if there is such a set.
     *
     * <p>
     * The implementation in {@link WordParser} returns the collection provided to the constructor, if any.
     * </p>
     *
     * @return collection of valid words, or null to not place any restriction
     */
    protected Collection<String> getWords() {
        return this.words;
    }

    @Override
    public String parse(Session session, ParseContext ctx, boolean complete) {

        // Get word
        final Matcher matcher = ctx.tryPattern("[^\\s;]*");
        if (matcher == null)
            throw new ParseException(ctx);
        final String word = matcher.group();

        // Check word
        final Collection<String> validWords = this.getWords();
        if (validWords != null) {
            final TreeSet<String> sortedWords = new TreeSet<>(validWords);
            if (!sortedWords.contains(word)) {
                throw new ParseException(ctx, "unknown " + this.description + " `" + word + "'")
                  .addCompletions(ParseUtil.complete(sortedWords, word));
            }
        } else if (word.length() == 0)
            throw new ParseException(ctx, "missing " + this.description);

        // Done
        return word;
    }
}

