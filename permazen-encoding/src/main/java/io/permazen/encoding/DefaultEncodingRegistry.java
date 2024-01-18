
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Permazen's default {@link EncodingRegistry}.
 *
 * <p>
 * Instances automatically register all of Permazen's built-in {@link Encoding}s (see below).
 *
 * <p><b>Array Types</b></p>
 *
 * <p>
 * Because this class is a subclass of {@link SimpleEncodingRegistry}, encodings for array types
 * are created on demand, so they don't need to be explicitly registered.
 *
 * <p><b>Enum Types</b></p>
 *
 * <p>
 * Encodings for {@link Enum} types are not registered in an {@link EncodingRegistry}. Instead, {@link Enum}
 * values are represented as {@link io.permazen.core.EnumValue} instances, and {@link Enum} fields are specially
 * defined in the schema with an explicit identifier list. In turn, {@link io.permazen.core.EnumValue}'s are
 * encoded by {@link io.permazen.core.EnumValueEncoding}.
 *
 * <p><b>Custom Encodings</b></p>
 *
 * <p>
 * During construction, instances scan the class path for custom {@link EncodingRegistry} implementations
 * and will delegate to them when an encoding is not found.
 *
 * <p>
 * If multiple custom {@link EncodingRegistry} implementations advertise the same encoding, one will be
 * chosen arbitrarily.
 *
 * <p>
 * Custom {@link EncodingRegistry} implemenations are specified via
 * {@code META-INF/services/io.permazen.encoding.EncodingRegistry} files or by module exports; see {@link ServiceLoader}.
 * Custom implementations are only queried for non-array types.
 *
 * <p><b>Built-in Encodings</b></p>
 *
 * <p>
 * Permazen provides built-in {@link Encoding}s covering the following Java types:
 * <ul>
 *  <li>Primitive types ({@code boolean}, {@code int}, etc.)</li>
 *  <li>Primitive array types ({@link boolean[]}, {@link int[]}, etc.)</li>
 *  <li>Primitive wrapper types ({@link Boolean}, {@link Integer}, etc.)</li>
 *  <li>{@link String}</li>
 *  <li>{@link java.math.BigDecimal}</li>
 *  <li>{@link java.math.BigInteger}</li>
 *  <li>{@link java.util.BitSet}</li>
 *  <li>{@link java.util.Date}</li>
 *  <li>{@link java.util.UUID}</li>
 *  <li>{@link java.net.URI}</li>
 *  <li>{@link java.io.File}</li>
 *  <li>{@link java.net.InetAddress}</li>
 *  <li>{@link java.net.Inet4Address}</li>
 *  <li>{@link java.net.Inet6Address}</li>
 *  <li>{@link java.util.regex.Pattern}</li>
 *  <li>{@link java.time java.time.*}</li>
 *  <li>{@link jakarta.mail.internet.InternetAddress} (if present on the classpath)</li>
 * </ul>
 */
public class DefaultEncodingRegistry extends SimpleEncodingRegistry {

    protected final ArrayList<EncodingRegistry> customEncodingRegistries = new ArrayList<>();

// Constructor

    /**
     * Constructor.
     *
     * <p>
     * This constructor invokes {@link #initialize}.
     */
    @SuppressWarnings("this-escape")
    public DefaultEncodingRegistry() {
        this.initialize();
//        System.out.println("ENCODINGS BY ID:");
//        new java.util.TreeMap<>(this.byId).forEach((k, v) -> System.out.println(String.format("    %-40s -> %s", k, v)));
//        System.out.println("ENCODINGS BY TYPETOKEN:");
//        this.byType.forEach((k, v) -> System.out.println(String.format("%50s -> %s", k, v)));
//        System.out.println("CUSTOM REGISTRIES:");
//        this.customEncodingRegistries.forEach(r -> System.out.println("    " + r));
    }

// EncodingRegistry

    @Override
    public EncodingId idForAlias(String alias) {
        return EncodingIds.idForAlias(alias);
    }

    @Override
    public String aliasForId(EncodingId encodingId) {
        return EncodingIds.aliasForId(encodingId);
    }

    @Override
    public synchronized Encoding<?> getEncoding(EncodingId encodingId) {

        // See if we have it
        Encoding<?> encoding = super.getEncoding(encodingId);
        if (encoding != null)
            return encoding;

        // Try custom registries
        encoding = this.customEncodingRegistries.stream()
          .map(registry -> registry.getEncoding(encodingId))
          .findFirst()
          .orElse(null);
        if (encoding != null)
            this.register(encodingId, encoding);

        // Done
        return encoding;
    }

    @Override
    public synchronized <T> List<Encoding<T>> getEncodings(TypeToken<T> typeToken) {

        // See if we have it
        List<Encoding<T>> encodingList = super.getEncodings(typeToken);
        if (!encodingList.isEmpty())
            return encodingList;

        // Try custom registries
        return this.customEncodingRegistries.stream()
          .map(registry -> registry.getEncodings(typeToken))
          .flatMap(List::stream)
          .peek(encoding -> this.register(encoding.getEncodingId(), encoding))
          .collect(Collectors.toList());
    }

// Internal Methods

    /**
     * Initialize this instance.
     *
     * <p>
     * This method is invoked by the default constructor. The implementation in {@link DefaultEncodingRegistry}
     * invokes {@link #addBuiltinEncodings} and then {@link #findCustomEncodingRegistries}.
     */
    protected void initialize() {
        this.addBuiltinEncodings();
        this.findCustomEncodingRegistries();
    }

    /**
     * Register Permazen's built-in encodings.
     */
    protected void addBuiltinEncodings() {

        // Primitive types
        this.add(new BooleanEncoding());
        this.add(new ByteEncoding());
        this.add(new CharacterEncoding());
        this.add(new DoubleEncoding());
        this.add(new FloatEncoding());
        this.add(new IntegerEncoding());
        this.add(new LongEncoding());
        this.add(new ShortEncoding());

        // Primitive wrapper types
        this.add(new PrimitiveWrapperEncoding<>(new BooleanEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new ByteEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new CharacterEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new DoubleEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new FloatEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new IntegerEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new LongEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new ShortEncoding()));
        this.add(new PrimitiveWrapperEncoding<>(new VoidEncoding()));

        // Primitive array types
        this.addNullSafe((new BooleanArrayEncoding()));
        this.addNullSafe((new ByteArrayEncoding()));
        this.addNullSafe((new CharacterArrayEncoding()));
        this.addNullSafe((new DoubleArrayEncoding()));
        this.addNullSafe((new FloatArrayEncoding()));
        this.addNullSafe((new IntegerArrayEncoding()));
        this.addNullSafe((new LongArrayEncoding()));
        this.addNullSafe((new ShortArrayEncoding()));

        // Types in java.lang
        this.addNullSafe((new StringEncoding()));

        // Types in java.math
        this.addNullSafe((new BigDecimalEncoding()));
        this.addNullSafe((new BigIntegerEncoding()));

        // Types in java.io
        this.addNullSafe(new FileEncoding());

        // Types in java.util
        this.addNullSafe((new BitSetEncoding()));
        this.addNullSafe((new DateEncoding()));
        this.addNullSafe((new UUIDEncoding()));

        // Types in java.util.regex
        this.addNullSafe(new PatternEncoding());

        // Types in java.net
        this.addNullSafe((new Inet4AddressEncoding()));
        this.addNullSafe((new Inet6AddressEncoding()));
        this.addNullSafe((new InetAddressEncoding()));
        this.addNullSafe((new URIEncoding()));

        // Types in java.time
        this.addNullSafe((new DurationEncoding()));
        this.addNullSafe((new InstantEncoding()));
        this.addNullSafe((new LocalDateTimeEncoding()));
        this.addNullSafe((new LocalDateEncoding()));
        this.addNullSafe((new LocalTimeEncoding()));
        this.addNullSafe((new MonthDayEncoding()));
        this.addNullSafe((new OffsetDateTimeEncoding()));
        this.addNullSafe((new OffsetTimeEncoding()));
        this.addNullSafe((new PeriodEncoding()));
        this.addNullSafe((new YearMonthEncoding()));
        this.addNullSafe((new YearEncoding()));
        this.addNullSafe((new ZoneOffsetEncoding()));
        this.addNullSafe((new ZonedDateTimeEncoding()));
        this.addNullSafe((new ZoneIdEncoding()));

        // Types that require optional classpath components
        try {
            this.addNullSafe((new InternetAddressEncoding()));
        } catch (NoClassDefFoundError e) {
            // ignore
        }
    }

    /**
     * Add a null-safe version of a type which is not null-safe.
     *
     * @param encoding non-null encoding
     * @throws IllegalArgumentException if {@code encoding} is an instance of {@link NullSafeEncoding}
     */
    protected <T> void addNullSafe(Encoding<T> encoding) {
        Preconditions.checkArgument(encoding != null, "null encoding");
        Preconditions.checkArgument(!(encoding instanceof NullSafeEncoding), "null safe encoding");
        this.add(new NullSafeEncoding<>(encoding));
    }

    /**
     * Scan the class path (via {@link ServiceLoader} and the current thread's context class loader)
     * for custom {@link EncodingRegistry} implementations
     */
    protected void findCustomEncodingRegistries() {
        for (Iterator<EncodingRegistry> i = ServiceLoader.load(EncodingRegistry.class).iterator(); i.hasNext(); ) {
            final EncodingRegistry customEncodingRegistry = i.next();
            this.log.debug("{}: including custom encoding registry {}", this.getClass().getSimpleName(), customEncodingRegistry);
            this.customEncodingRegistries.add(customEncodingRegistry);
        }
    }
}
