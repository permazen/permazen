
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import java.util.Optional;

/**
 * {@link EncodingId}'s for Permazen built-in encodings.
 *
 * @see EncodingId
 */
public final class EncodingIds {

    /**
     * Prefix for Permazen built-in encodings.
     */
    public static final String PERMAZEN_PREFIX = "urn:fdc:permazen.io:2020:";

    private EncodingIds() {
    }

    /**
     * Build an encoding ID for a built-in Permazen encoding, using the given suffix.
     *
     * @param suffix ID suffix
     * @throws IllegalArgumentException if {@code suffix} is null or invalid
     */
    public static EncodingId builtin(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        Preconditions.checkArgument(EncodingIds.isValidBuiltinSuffix(suffix), "invalid suffix");
        return new EncodingId(PERMAZEN_PREFIX + suffix);
    }

    /**
     * Determine if the given string is a valid Permazen built-in encoding ID suffix.
     *
     * <p>
     * Valid suffixes must start with an ASCII letter; contain only ASCII letters,
     * digits, underscores, and dashes; not end with a dash; and not have any
     * consecutive dashes. They may have up to 255 trailing {@code []} array dimensions.
     *
     * @param suffix ID suffix
     * @return true if {@code suffix} is a valid suffix, false otherwise
     * @throws IllegalArgumentException if {@code suffix} is null
     */
    public static boolean isValidBuiltinSuffix(String suffix) {
        Preconditions.checkArgument(suffix != null, "null suffix");
        return suffix.matches("\\p{Alpha}(-?[\\p{Alnum}_]+)*(\\[\\]){0,255}");
    }

    /**
     * Get the encoding ID corresponding to the given alias (or "nickname"), if any,
     * for a Permazen built-in encoding.
     *
     * <p>
     * This implements the default logic for {@link DefaultEncodingRegistry#idForAlias DefaultEncodingRegistry.idForAlias()}:
     * If {@code alias} satisfies {@link #isValidBuiltinSuffix isValidBuiltinSuffix()}, then this method
     * delegates to {@link #builtin builtin()}, otherwise to {@link EncodingId#EncodingId(String) new EncodingId()}.
     *
     * @param alias encoding ID alias
     * @return corresponding encoding ID, never null
     * @throws IllegalArgumentException if {@code alias} is null or not a valid alias
     */
    public static EncodingId idForAlias(String alias) {
        Preconditions.checkArgument(alias != null, "null alias");
        if (EncodingIds.isValidBuiltinSuffix(alias))
            return EncodingIds.builtin(alias);
        return new EncodingId(alias);
    }

    /**
     * Get the alias (or "nickname") for the given encoding ID in this registry, if any,
     * for a Permazen built-in encoding.
     *
     * <p>
     * This implements the default logic for {@link DefaultEncodingRegistry#aliasForId DefaultEncodingRegistry.aliasForId()}:
     * If {@code encodingId} equals {@value EncodingIds#PERMAZEN_PREFIX} followed by a suffix satisfying
     * {@link #isValidBuiltinSuffix isValidBuiltinSuffix()}, then the suffix is returned, otherwise {@code encodingId}
     * in string form is returned.
     *
     * @param encodingId encoding ID
     * @return corresponding alias, if any, otherwise {@link EncodingId#getId}
     * @throws IllegalArgumentException if {@code encodingId} is null
     */
    public static String aliasForId(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null, "null encodingId");
        final String id = encodingId.getId();
        return Optional.of(id)
          .filter(s -> s.startsWith(EncodingIds.PERMAZEN_PREFIX))
          .map(s -> s.substring(EncodingIds.PERMAZEN_PREFIX.length()))
          .filter(EncodingIds::isValidBuiltinSuffix)
          .orElse(id);
    }
}
