
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.annotation.PermazenType;
import io.permazen.core.Database;
import io.permazen.encoding.DefaultEncodingRegistry;
import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.simple.MemoryKVDatabase;

import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Configuration object used to create new {@link Permazen} instances.
 *
 * @see Permazen
 */
public final class PermazenConfig {

    // Config information
    private final Database database;
    private final EncodingRegistry encodingRegistry;
    private final ValidatorFactory validatorFactory;
    private final LinkedHashSet<Class<?>> modelClasses;

// Constructor

    private PermazenConfig(Builder builder) {
        assert builder != null;
        Preconditions.checkArgument(builder.modelClasses != null, "no Java model classes have been configured");
        this.database = Optional.ofNullable(builder.database).orElseGet(() -> new Database(new MemoryKVDatabase()));
        this.encodingRegistry = Optional.ofNullable(builder.encodingRegistry).orElseGet(DefaultEncodingRegistry::new);
        this.validatorFactory = builder.validatorFactory;
        this.modelClasses = builder.modelClasses;
    }

// Property Methods

    /**
     * Get the underlying {@link Database} to be used.
     *
     * @return core API database to use
     */
    public Database getDatabase() {
        return this.database;
    }

    /**
     * Get the {@link EncodingRegistry} to be used.
     *
     * @return {@link EncodingRegistry} for finding simple field encodings
     */
    public EncodingRegistry getEncodingRegistry() {
        return this.encodingRegistry;
    }

    /**
     * Get the {@link ValidatorFactory} that will create the {@link Validator}s used for validation within transactions.
     *
     * @return factory for validators, or null to use the default
     */
    public ValidatorFactory getValidatorFactory() {
        return this.validatorFactory;
    }

    /**
     * Get the Java model classes to be used.
     *
     * @return Java database model classes
     */
    public Set<Class<?>> getModelClasses() {
        return Collections.unmodifiableSet(this.modelClasses);
    }

// Other Methods

    /**
     * Create a {@link Builder}.
     *
     * @return new {@link PermazenConfig} builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Convenience method that uses this instance to create a new {@link Permazen} instance.
     *
     * @return new database instance using this configuration
     * @see Permazen#Permazen(PermazenConfig)
     */
    public Permazen newPermazen() {
        return new Permazen(this);
    }

// Builder

    /**
     * Builder for {@link PermazenConfig}s.
     */
    public static final class Builder implements Cloneable {

        private Database database;
        private EncodingRegistry encodingRegistry;
        private ValidatorFactory validatorFactory;
        private LinkedHashSet<Class<?>> modelClasses;

    // Constructors

        private Builder() {
        }

    // Methods

        /**
         * Configure the underlying {@link Database}.
         *
         * <p>
         * By default an initially empty, in-memory {@link MemoryKVDatabase} is used.
         *
         * @param database core API database to use
         * @return this instance
         */
        public Builder database(Database database) {
            this.database = database;
            return this;
        }

        /**
         * Configure the {@link EncodingRegistry}.
         *
         * @param encodingRegistry custom {@link EncodingRegistry}, or null for the default
         * @return this instance
         */
        public Builder encodingRegistry(EncodingRegistry encodingRegistry) {
            this.encodingRegistry = encodingRegistry;
            return this;
        }

        /**
         * Configure a custom {@link ValidatorFactory} used to create {@link Validator}s for validation within transactions.
         *
         * <p>
         * The default is to use the result from {@link Validation#buildDefaultValidatorFactory}.
         *
         * @param validatorFactory factory for validators, or null for the default
         * @return this instance
         */
        public Builder validatorFactory(ValidatorFactory validatorFactory) {
            this.validatorFactory = validatorFactory;
            return this;
        }

        /**
         * Configure the Java model classes via iteration.
         *
         * <p>
         * Note: {@link PermazenType &#64;PermazenType}-annotated super-types of any class in {@code modelClasses}
         * will be included, even if the super-type is not explicitly specified in {@code modelClasses}.
         *
         * @param modelClasses classes annotated with {@link PermazenType &#64;PermazenType} annotations
         * @return this instance
         * @throws IllegalArgumentException if {@code modelClasses} is null or contains a null class
         */
        public Builder modelClasses(Iterable<Class<?>> modelClasses) {
            Preconditions.checkArgument(modelClasses != null, "null modelClasses");
            final LinkedHashSet<Class<?>> set = new LinkedHashSet<>();
            for (Class<?> modelClass : modelClasses) {
                Preconditions.checkArgument(modelClass != null, "null model class in set");
                set.add(modelClass);
            }
            this.modelClasses = set;
            return this;
        }

        /**
         * Configure the Java model classes from an array.
         *
         * <p>
         * Note: {@link PermazenType &#64;PermazenType}-annotated super-types of any class in {@code modelClasses}
         * will be included, even if the super-type is not explicitly specified in {@code modelClasses}.
         *
         * @param modelClasses classes annotated with {@link PermazenType &#64;PermazenType} annotations
         * @return this instance
         * @throws IllegalArgumentException if {@code modelClasses} is null or contains a null class
         */
        public Builder modelClasses(Class<?>... modelClasses) {
            Preconditions.checkArgument(modelClasses != null, "null modelClasses");
            return this.modelClasses(Arrays.asList(modelClasses));
        }

        /**
         * Create a new {@link PermazenConfig} from this instance.
         *
         * @return new {@link PermazenConfig}
         * @throws IllegalArgumentException if a model class has invalid annotation(s)
         * @throws io.permazen.core.InvalidSchemaException if the schema derived from the model classes is invalid
         */
        public PermazenConfig build() {
            return new PermazenConfig(this);
        }

        /**
         * Clone this instance.
         *
         * @return clone of this instance
         */
        public Builder clone() {
            final Builder clone;
            try {
                clone = (Builder)super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            if (clone.modelClasses != null)
                clone.modelClasses = new LinkedHashSet<>(clone.modelClasses);
            return clone;
        }
    }
}
