
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;

import io.permazen.core.Database;
import io.permazen.core.FieldTypeRegistry;
import io.permazen.kv.simple.SimpleKVDatabase;

import jakarta.validation.ValidatorFactory;

import java.util.Arrays;
import java.util.Collection;

/**
 * Factory for {@link Permazen} instances.
 *
 * <p>
 * If no {@link Database} is configured, newly created {@link Permazen} instances will use an initially empty,
 * in-memory {@link SimpleKVDatabase}.
 *
 * @see Permazen
 */
public class PermazenFactory {

    private Database database;
    private int schemaVersion = -1;
    private FieldTypeRegistry fieldTypeRegistry;
    private StorageIdGenerator storageIdGenerator = new DefaultStorageIdGenerator();
    private Collection<Class<?>> modelClasses;
    private ValidatorFactory validatorFactory;

    /**
     * Configure the Java model classes.
     *
     * <p>
     * Note: {@link io.permazen.annotation.PermazenType &#64;PermazenType}-annotated super-types of any
     * class in {@code modelClasses} will be included, even if the super-type is not explicitly specified in {@code modelClasses}.
     *
     * @param modelClasses classes annotated with {@link io.permazen.annotation.PermazenType &#64;PermazenType} annotations
     * @return this instance
     */
    public PermazenFactory setModelClasses(Collection<Class<?>> modelClasses) {
        this.modelClasses = modelClasses;
        return this;
    }

    /**
     * Configure the Java model classes.
     *
     * <p>
     * Equivalent to {@link #setModelClasses(Collection) setModelClasses}{@code (Arrays.asList(modelClasses))}.
     *
     * @param modelClasses classes annotated with {@link io.permazen.annotation.PermazenType &#64;PermazenType} annotations
     * @return this instance
     * @see #setModelClasses(Collection)
     */
    public PermazenFactory setModelClasses(Class<?>... modelClasses) {
        return this.setModelClasses(Arrays.asList(modelClasses));
    }

    /**
     * Configure the underlying {@link Database} for this instance.
     *
     * <p>
     * By default this instance will use an initially empty, in-memory {@link SimpleKVDatabase}.
     *
     * @param database core API database to use
     * @return this instance
     */
    public PermazenFactory setDatabase(Database database) {
        this.database = database;
        return this;
    }

    /**
     * Configure the schema version number associated with the configured Java model classes.
     *
     * <p>
     * A value of zero means to use whatever is the highest version already recorded in the database.
     *
     * <p>
     * A value of -1 means to {@linkplain io.permazen.schema.SchemaModel#autogenerateVersion auto-generate}
     * a version number based on the {@linkplain io.permazen.schema.SchemaModel#compatibilityHash compatibility hash}
     * of the {@link io.permazen.schema.SchemaModel} generated from the {@linkplain #setModelClasses configured model classes}.
     *
     * <p>
     * Default is -1.
     *
     * @param schemaVersion the schema version number of the schema derived from the configured Java model classes,
     *  zero to use the highest version already recorded in the database,
     *  or -1 to use an {@linkplain io.permazen.schema.SchemaModel#autogenerateVersion auto-generated} schema version
     * @return this instance
     */
    public PermazenFactory setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
        return this;
    }

    /**
     * Configure a custom {@link FieldTypeRegistry}.
     *
     * @param fieldTypeRegistry custom {@link FieldTypeRegistry}, or null for the default
     * @return this instance
     */
    public PermazenFactory setFieldTypeRegistry(FieldTypeRegistry fieldTypeRegistry) {
        this.fieldTypeRegistry = fieldTypeRegistry;
        return this;
    }

    /**
     * Configure the {@link StorageIdGenerator} for auto-generating storage ID's when not explicitly
     * specified in {@link io.permazen.annotation.PermazenType &#64;PermazenType},
     * {@link io.permazen.annotation.JField &#64;JField}, etc., annotations.
     *
     * <p>
     * This instance will initially be configured with a {@link DefaultStorageIdGenerator}.
     * To disable auto-generation of storage ID's altogether, configure a null value here.
     *
     * @param storageIdGenerator storage ID generator, or null to disable auto-generation of storage ID's
     * @return this instance
     */
    public PermazenFactory setStorageIdGenerator(StorageIdGenerator storageIdGenerator) {
        this.storageIdGenerator = storageIdGenerator;
        return this;
    }

    /**
     * Configure a custom {@link ValidatorFactory} used to create {@link jakarta.validation.Validator}s
     * for validation within transactions.
     *
     * <p>
     * The default is to use the result from {@link jakarta.validation.Validation#buildDefaultValidatorFactory}.
     *
     * @param validatorFactory factory for validators
     * @return this instance
     * @throws IllegalArgumentException if {@code validatorFactory} is null
     */
    public PermazenFactory setValidatorFactory(ValidatorFactory validatorFactory) {
        Preconditions.checkArgument(validatorFactory != null, "null validatorFactory");
        this.validatorFactory = validatorFactory;
        return this;
    }

    /**
     * Construct a {@link Permazen} instance using this instance's configuration.
     *
     * @return newly created {@link Permazen} database
     * @throws IllegalArgumentException if this instance has an incomplete or invalid configuration
     * @throws IllegalArgumentException if any Java model class has an invalid annotation
     */
    public Permazen newPermazen() {
        Database database1 = this.database;
        if (database1 == null)
            database1 = new Database(new SimpleKVDatabase());
        if (this.fieldTypeRegistry != null)
            database1.setFieldTypeRegistry(this.fieldTypeRegistry);
        final Permazen jdb = new Permazen(database1, this.schemaVersion, this.storageIdGenerator, this.modelClasses);
        if (this.validatorFactory != null)
            jdb.setValidatorFactory(this.validatorFactory);
        return jdb;
    }
}
