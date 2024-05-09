
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.io.File;
import java.net.URI;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.function.Function;
import java.util.regex.Pattern;
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
          .filter(Objects::nonNull)
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
     *
     * <p>
     * The implementation in {@link DefaultEncodingRegistry} invokes {@link #addStandardBuiltinEncodings}
     * and then {@link #addOptionalBuiltinEncodings}.
     */
    protected void addBuiltinEncodings() {
        this.addStandardBuiltinEncodings();
        this.addOptionalBuiltinEncodings();
    }

    /**
     * Register Permazen's standard built-in encodings.
     */
    protected void addStandardBuiltinEncodings() {

        // Get primitive type EncodingId's
        final EncodingId z = EncodingIds.builtin("boolean");
        final EncodingId b = EncodingIds.builtin("byte");
        final EncodingId c = EncodingIds.builtin("char");
        final EncodingId d = EncodingIds.builtin("double");
        final EncodingId f = EncodingIds.builtin("float");
        final EncodingId i = EncodingIds.builtin("int");
        final EncodingId j = EncodingIds.builtin("long");
        final EncodingId s = EncodingIds.builtin("short");

        // Add primitive types
        this.add(new BooleanEncoding(z));
        this.add(new ByteEncoding(b));
        this.add(new CharacterEncoding(c));
        this.add(new DoubleEncoding(d));
        this.add(new FloatEncoding(f));
        this.add(new IntegerEncoding(i));
        this.add(new LongEncoding(j));
        this.add(new ShortEncoding(s));

        // Add primitive wrapper types
        this.add(new PrimitiveWrapperEncoding<>(new BooleanEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new ByteEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new CharacterEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new DoubleEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new FloatEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new IntegerEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new LongEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new ShortEncoding(null)));
        this.add(new PrimitiveWrapperEncoding<>(new VoidEncoding(null)));

        // Add primitive array types
        this.addWrapped(z.getArrayId(), new BooleanArrayEncoding(null));
        this.addWrapped(b.getArrayId(), new ByteArrayEncoding(null));
        this.addWrapped(c.getArrayId(), new CharacterArrayEncoding(null));
        this.addWrapped(d.getArrayId(), new DoubleArrayEncoding(null));
        this.addWrapped(f.getArrayId(), new FloatArrayEncoding(null));
        this.addWrapped(i.getArrayId(), new IntegerArrayEncoding(null));
        this.addWrapped(j.getArrayId(), new LongArrayEncoding(null));
        this.addWrapped(s.getArrayId(), new ShortArrayEncoding(null));

        // Built-in types in java.lang
        this.addWrappedBuiltin(new StringEncoding(null));

        // Built-in types in java.math
        this.addWrappedBuiltin(new BigDecimalEncoding(null));
        this.addWrappedBuiltin(new BigIntegerEncoding(null));

        // Built-in types in java.io
        this.addBuiltin(File.class, FileEncoding::new);

        // Built-in types in java.util
        this.addWrappedBuiltin(new BitSetEncoding(null));
        this.addWrappedBuiltin(new DateEncoding(null));
        this.addWrappedBuiltin(new UUIDEncoding(null));

        // Built-in types in java.util.regex
        this.addBuiltin(Pattern.class, PatternEncoding::new);

        // Built-in types in java.net
        this.addWrappedBuiltin(new Inet4AddressEncoding(null));
        this.addWrappedBuiltin(new Inet6AddressEncoding(null));
        this.addWrappedBuiltin(new InetAddressEncoding(null));
        this.addBuiltin(URI.class, URIEncoding::new);

        // Built-in types in java.time
        this.addWrappedBuiltin(new DurationEncoding(null));
        this.addWrappedBuiltin(new InstantEncoding(null));
        this.addWrappedBuiltin(new LocalDateTimeEncoding(null));
        this.addWrappedBuiltin(new LocalDateEncoding(null));
        this.addWrappedBuiltin(new LocalTimeEncoding(null));
        this.addWrappedBuiltin(new MonthDayEncoding(null));
        this.addWrappedBuiltin(new OffsetDateTimeEncoding(null));
        this.addWrappedBuiltin(new OffsetTimeEncoding(null));
        this.addWrappedBuiltin(new PeriodEncoding(null));
        this.addWrappedBuiltin(new YearMonthEncoding(null));
        this.addWrappedBuiltin(new YearEncoding(null));
        this.addWrappedBuiltin(new ZoneOffsetEncoding(null));
        this.addWrappedBuiltin(new ZonedDateTimeEncoding(null));
        this.addBuiltin(ZoneId.class, ZoneIdEncoding::new);
    }

    private <T> void addBuiltin(Class<T> type, Function<EncodingId, ? extends Encoding<T>> ctor) {
        final Encoding<T> encoding = ctor.apply(EncodingIds.builtin(type.getSimpleName()));
        Preconditions.checkArgument(encoding.supportsNull(), "encoding does not support null");
        this.add(encoding);
    }

    private void addWrappedBuiltin(Encoding<?> encoding) {
        final Class<?> javaType = encoding.getTypeToken().getRawType();
        final EncodingId encodingId = EncodingIds.builtin(javaType.getSimpleName());
        this.addWrapped(encodingId, encoding);
    }

    private <T> void addWrapped(EncodingId encodingId, Encoding<T> encoding) {
        Preconditions.checkArgument(encoding != null, "null encoding");
        this.add(new NullSafeEncoding<>(encodingId, encoding));
    }

    /**
     * Register Permazen's optional built-in encodings.
     *
     * <p>
     * The optional built-in encodings are ones that depend on optional dependencies. An example is
     * {@link InternetAddressEncoding} which depends on the Jakarta Mail API.
     */
    protected void addOptionalBuiltinEncodings() {
        this.addOptionalBuiltinEncoding("InternetAddress", InternetAddressEncoding::new);
    }

    /**
     * Register a built-in encoding, but only if its target class is found on the classpath.
     *
     * <p>
     * The implementation in {@link DefaultEncodingRegistry} invokes the given {@code builder} but will catch
     * and ignore any {@link NoClassDefFoundError} thrown. Otherwise, the encoding is registered.
     *
     * <p>
     * No attempt to <a href="https://docs.oracle.com/javase/specs/jls/se9/html/jls-12.html#jls-12.4.1">initialize</a>
     * the encoding class should have occurred prior to invoking this method.
     *
     * @param name builtin encoding ID suffix
     * @param builder builder for encoding
     */
    protected void addOptionalBuiltinEncoding(String name, Function<EncodingId, ? extends Encoding<?>> builder) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(builder != null, "null builder");
        final EncodingId encodingId = EncodingIds.builtin(name);
        final Encoding<?> encoding;
        try {
            encoding = builder.apply(encodingId);
        } catch (NoClassDefFoundError e) {
            this.log.debug("{}: not adding optional encoding \"{}\": {}",
              this.getClass().getSimpleName(), encodingId, e.toString());
            return;
        }
        Preconditions.checkArgument(encoding != null, "null encoding returned from builder");
        this.add(encoding);
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
