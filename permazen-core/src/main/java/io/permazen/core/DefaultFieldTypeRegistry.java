
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.type.BigDecimalType;
import io.permazen.core.type.BigIntegerType;
import io.permazen.core.type.BitSetType;
import io.permazen.core.type.BooleanArrayType;
import io.permazen.core.type.ByteArrayType;
import io.permazen.core.type.CharacterArrayType;
import io.permazen.core.type.DateType;
import io.permazen.core.type.DoubleArrayType;
import io.permazen.core.type.DurationType;
import io.permazen.core.type.EnumValueFieldType;
import io.permazen.core.type.FileType;
import io.permazen.core.type.FloatArrayType;
import io.permazen.core.type.Inet4AddressType;
import io.permazen.core.type.Inet6AddressType;
import io.permazen.core.type.InetAddressType;
import io.permazen.core.type.InstantType;
import io.permazen.core.type.IntegerArrayType;
import io.permazen.core.type.InternetAddressType;
import io.permazen.core.type.LocalDateTimeType;
import io.permazen.core.type.LocalDateType;
import io.permazen.core.type.LocalTimeType;
import io.permazen.core.type.LongArrayType;
import io.permazen.core.type.MonthDayType;
import io.permazen.core.type.NullSafeType;
import io.permazen.core.type.OffsetDateTimeType;
import io.permazen.core.type.OffsetTimeType;
import io.permazen.core.type.PatternType;
import io.permazen.core.type.PeriodType;
import io.permazen.core.type.PrimitiveWrapperType;
import io.permazen.core.type.ShortArrayType;
import io.permazen.core.type.StringType;
import io.permazen.core.type.URIType;
import io.permazen.core.type.UUIDType;
import io.permazen.core.type.YearMonthType;
import io.permazen.core.type.YearType;
import io.permazen.core.type.ZoneIdType;
import io.permazen.core.type.ZoneOffsetType;
import io.permazen.core.type.ZonedDateTimeType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

/**
 * Permazen's default {@link FieldTypeRegistry}.
 *
 * <p>
 * Instances automatically register all of Permazen's built-in {@link FieldType}s (see below).
 *
 * <p><b>Array Types</b></p>
 *
 * <p>
 * Because this class is a subclass of {@link SimpleFieldTypeRegistry}, array types are created
 * on demand so they don't need to be explicitly registered.
 *
 * <p><b>Custom Types</b></p>
 *
 * <p>
 * When constructed, instances scan the class path for custom {@link FieldTypeRegistry} implementations
 * and will delegate to them when an encoding is not found. If multiple custom {@link FieldTypeRegistry}
 * implementations advertise the same encoding, one will be chosen arbitrarily.
 *
 * <p>
 * Custom {@link FieldTypeRegistry} implemenations are specified via
 * {@code META-INF/services/io.permazen.core.FieldTypeRegistry} files or by module exports; see {@link ServiceLoader}.
 * Custom implementations are only queried for non-array types.
 *
 * <p><b>Built-in Types</b></p>
 *
 * <p>
 * Permazen's built-in {@link FieldType}s cover the following Java types:
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
 *
 * <p>
 * {@link Enum} types are not directly handled in the core API layer; instead, the appropriate
 * {@link EnumValueFieldType} encodes enum values as {@link EnumValue}s.
 */
public class DefaultFieldTypeRegistry extends SimpleFieldTypeRegistry {

    protected final ArrayList<FieldTypeRegistry> customFieldTypeRegistries = new ArrayList<>();

// Constructor

    public DefaultFieldTypeRegistry() {
        this.addBuiltinFieldTypes();
        this.findCustomFieldTypeRegistries();
//        System.out.println("TYPES BY ID:");
//        new java.util.TreeMap<>(this.typesById).forEach((k, v) -> System.out.println(String.format("    %-40s -> %s", k, v)));
//        System.out.println("TYPES BY TYPETOKEN:");
//        this.typesByType.forEach((k, v) -> System.out.println(String.format("%50s -> %s", k, v)));
//        System.out.println("CUSTOM REGISTRIES:");
//        this.customFieldTypeRegistries.forEach(r -> System.out.println("    " + r));
    }

// FieldTypeRegistry

    @Override
    public EncodingId idForAlias(String alias) {
        Preconditions.checkArgument(alias != null, "null alias");
        if (alias.indexOf(':') == -1)                    // a very basic filter
            return EncodingIds.builtin(alias);
        return new EncodingId(alias);
    }

    @Override
    public String aliasForId(EncodingId encodingId) {
        Preconditions.checkArgument(encodingId != null, "null encodingId");
        final String id = encodingId.getId();
        return Optional.of(id)
          .filter(s -> s.startsWith(EncodingIds.PERMAZEN_PREFIX))
          .map(s -> s.substring(EncodingIds.PERMAZEN_PREFIX.length()))
          .orElse(id);
    }

    @Override
    public synchronized FieldType<?> getFieldType(EncodingId encodingId) {

        // See if we have it
        FieldType<?> fieldType = super.getFieldType(encodingId);
        if (fieldType != null)
            return fieldType;

        // Try custom registries
        fieldType = this.customFieldTypeRegistries.stream()
          .map(registry -> registry.getFieldType(encodingId))
          .findFirst()
          .orElse(null);
        if (fieldType != null)
            this.register(encodingId, fieldType);

        // Done
        return fieldType;
    }

    @Override
    public synchronized <T> List<FieldType<T>> getFieldTypes(TypeToken<T> typeToken) {

        // See if we have it
        List<FieldType<T>> fieldTypeList = super.getFieldTypes(typeToken);
        if (!fieldTypeList.isEmpty())
            return fieldTypeList;

        // Try custom registries
        return this.customFieldTypeRegistries.stream()
          .map(registry -> registry.getFieldTypes(typeToken))
          .flatMap(List::stream)
          .peek(fieldType -> this.register(fieldType.getEncodingId(), fieldType))
          .collect(Collectors.toList());
    }

// Internal Methods

    /**
     * Register Permazen's built-in types.
     */
    protected void addBuiltinFieldTypes() {

        // Primitive types
        this.add(Encodings.BOOLEAN);
        this.add(Encodings.BYTE);
        this.add(Encodings.CHARACTER);
        this.add(Encodings.DOUBLE);
        this.add(Encodings.FLOAT);
        this.add(Encodings.INTEGER);
        this.add(Encodings.LONG);
        this.add(Encodings.SHORT);

        // Primitive wrapper types
        this.add(new PrimitiveWrapperType<>(Encodings.BOOLEAN));
        this.add(new PrimitiveWrapperType<>(Encodings.BYTE));
        this.add(new PrimitiveWrapperType<>(Encodings.CHARACTER));
        this.add(new PrimitiveWrapperType<>(Encodings.DOUBLE));
        this.add(new PrimitiveWrapperType<>(Encodings.FLOAT));
        this.add(new PrimitiveWrapperType<>(Encodings.INTEGER));
        this.add(new PrimitiveWrapperType<>(Encodings.LONG));
        this.add(new PrimitiveWrapperType<>(Encodings.SHORT));
        this.add(new PrimitiveWrapperType<>(Encodings.VOID));

        // Primitive array types
        this.add(new NullSafeType<>(new BooleanArrayType()));
        this.add(new NullSafeType<>(new ByteArrayType()));
        this.add(new NullSafeType<>(new CharacterArrayType()));
        this.add(new NullSafeType<>(new DoubleArrayType()));
        this.add(new NullSafeType<>(new FloatArrayType()));
        this.add(new NullSafeType<>(new IntegerArrayType()));
        this.add(new NullSafeType<>(new LongArrayType()));
        this.add(new NullSafeType<>(new ShortArrayType()));

        // Types in java.lang
        this.add(new NullSafeType<>(new StringType()));

        // Types in java.math
        this.add(new NullSafeType<>(new BigDecimalType()));
        this.add(new NullSafeType<>(new BigIntegerType()));

        // Types in java.io
        this.add(new FileType());

        // Types in java.util
        this.add(new NullSafeType<>(new BitSetType()));
        this.add(new NullSafeType<>(new DateType()));
        this.add(new NullSafeType<>(new UUIDType()));

        // Types in java.util.regex
        this.add(new PatternType());

        // Types in java.net
        this.add(new NullSafeType<>(new Inet4AddressType()));
        this.add(new NullSafeType<>(new Inet6AddressType()));
        this.add(new NullSafeType<>(new InetAddressType()));
        this.add(new URIType());

        // Types in java.time
        this.add(new NullSafeType<>(new DurationType()));
        this.add(new NullSafeType<>(new InstantType()));
        this.add(new NullSafeType<>(new LocalDateTimeType()));
        this.add(new NullSafeType<>(new LocalDateType()));
        this.add(new NullSafeType<>(new LocalTimeType()));
        this.add(new NullSafeType<>(new MonthDayType()));
        this.add(new NullSafeType<>(new OffsetDateTimeType()));
        this.add(new NullSafeType<>(new OffsetTimeType()));
        this.add(new NullSafeType<>(new PeriodType()));
        this.add(new NullSafeType<>(new YearMonthType()));
        this.add(new NullSafeType<>(new YearType()));
        this.add(new NullSafeType<>(new ZoneOffsetType()));
        this.add(new NullSafeType<>(new ZonedDateTimeType()));
        this.add(new ZoneIdType());

        // Types that require optional classpath components
        try {
            this.add(new InternetAddressType());
        } catch (NoClassDefFoundError e) {
            // ignore
        }
    }

    /**
     * Scan the class path (via {@link ServiceLoader} and the current thread's context class loader)
     * for custom {@link FieldTypeRegistry} implementations
     */
    protected void findCustomFieldTypeRegistries() {
        for (Iterator<FieldTypeRegistry> i = ServiceLoader.load(FieldTypeRegistry.class).iterator(); i.hasNext(); )
            this.customFieldTypeRegistries.add(i.next());
    }
}
