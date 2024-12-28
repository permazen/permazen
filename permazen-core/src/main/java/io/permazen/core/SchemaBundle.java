
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import io.permazen.encoding.EncodingRegistry;
import io.permazen.kv.KVPair;
import io.permazen.kv.KVStore;
import io.permazen.kv.KeyRanges;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.CloseableIterator;
import io.permazen.util.ImmutableNavigableMap;
import io.permazen.util.UnsignedIntEncoder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reflects the Schema table currently recorded in a {@link Database} as seen by a particular {@link Transaction}.
 * This includes any {@link Schema}s that may have been added or deleted during the transaction.
 *
 * <p>
 * This class also captures the current storage ID and schema index tables.
 *
 * <p>
 * Instances are immutable and thread safe.
 */
public class SchemaBundle {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    // Raw byte[] data read from database
    private final Encoded encoded;

    // Where to find encodings
    private final EncodingRegistry encodingRegistry;

    // Tables read from database
    private final TreeMap<Integer, Schema> schemasBySchemaIndex = new TreeMap<>();          // Schema table
    private final TreeMap<Integer, SchemaId> schemaIdsByStorageId = new TreeMap<>();        // Storage ID table

    // Schemas keyed by schema ID
    private final HashMap<SchemaId, Schema> schemasBySchemaId = new HashMap<>();

    // Storage ID's keyed by schema ID (inverse of schemaIdsByStorageId)
    private final HashMap<SchemaId, Integer> storageIdsBySchemaId = new HashMap<>();

    // Maps object type names (in any schema) -> corresponding storage ID
    private final HashMap<String, Integer> storageIdsByTypeName = new HashMap<>();

    // Maps schema ID -> representative SchemaItem from a randomly chosen schema
    private final HashMap<SchemaId, SchemaItem> schemaItemsBySchemaId = new HashMap<>();

    // Maps storage ID -> object type name
    private final HashMap<Integer, String> typeNamesByStorageId = new HashMap<>();

    // All object type storage ID's
    private final TreeSet<Integer> objTypeStorageIds = new TreeSet<>();

    // The set of key ranges corresponding to all ObjTypes
    private KeyRanges objTypesKeyRanges;

// Constructors

    /**
     * Partial constructor for debugging and tooling purposes.
     *
     * <p>
     * This constructor does not create a functional instance (no {@link EncodingRegistry} provided).
     * However, it does perform enough initialization to allow schema-related meta-data to be discovered.
     *
     * @param encoded encoded schema and storage ID tables
     * @throws InconsistentDatabaseException if the encoded data is invalid
     * @throws IllegalArgumentException if {@code encoded} is null
     */
    public SchemaBundle(Encoded encoded) {
        this(encoded, null, false);
    }

    /**
     * Constructor.
     *
     * @param encoded encoded schema and storage ID tables
     * @param encodingRegistry registry for simple field encodings
     * @throws InconsistentDatabaseException if the encoded data is invalid
     * @throws IllegalArgumentException if either parameter is null
     */
    public SchemaBundle(Encoded encoded, EncodingRegistry encodingRegistry) {
        this(encoded, encodingRegistry, true);
    }

    @SuppressWarnings("this-escape")
    private SchemaBundle(Encoded encoded, EncodingRegistry encodingRegistry, boolean encodingRegistryRequired) {

        // Sanity check
        Preconditions.checkArgument(encoded != null, "null encoded");
        Preconditions.checkArgument(!encodingRegistryRequired || encodingRegistry != null, "null encodingRegistry");

        // Initialize
        this.encoded = encoded;
        this.encodingRegistry = encodingRegistry;

        // Decode the storage ID table
        try {
            this.encoded.decodeStorageIdTable(this);
        } catch (IllegalArgumentException e) {
            throw new InconsistentDatabaseException(String.format(
              "invalid schema meta-data found in database: %s", e.getMessage()), e);
        }

        // Build mapping from SchemaId -> storage ID and verify each schema ID only appears once in the storage ID table
        for (Map.Entry<Integer, SchemaId> entry : this.schemaIdsByStorageId.entrySet()) {
            final int storageId = (int)entry.getKey();
            final SchemaId schemaId = entry.getValue();
            final Integer storageId0 = this.storageIdsBySchemaId.put(schemaId, storageId);
            if (storageId0 != null) {
                throw new InconsistentDatabaseException(String.format(
                  "schema ID \"%s\" has multiple assigned storage ID's: %d and %d", schemaId, storageId0, storageId));
            }
        }
        assert this.schemaIdsByStorageId.size() == this.storageIdsBySchemaId.size();

        // Decode the schema table
        try {
            this.encoded.decodeSchemaTable(this);
        } catch (IllegalArgumentException e) {
            throw new InconsistentDatabaseException(String.format(
              "invalid schema meta-data found in database: %s", e.getMessage()), e);
        }

        // Build mapping from SchemaId -> Schema and verify each schema only appears once in the schema table
        for (Schema schema : this.schemasBySchemaIndex.values()) {
            final SchemaId schemaId = schema.getSchemaId();
            final Schema schema0 = this.schemasBySchemaId.put(schemaId, schema);
            if (schema0 != null) {
                throw new InconsistentDatabaseException(String.format(
                  "schema \"%s\" is recorded at multiple schema indexes: %d and %d)",
                  schemaId, schema0.getSchemaIndex(), schema.getSchemaIndex()));
            }
        }
        assert this.schemasBySchemaIndex.size() == this.schemasBySchemaId.size();

        // Verify that every SchemaItem in every Schema has some assigned storage ID
        final TreeMap<Integer, AtomicInteger> refsMap = SchemaBundle.countStorageIdRefs(
          this.schemaIdsByStorageId.keySet(), this.schemasBySchemaIndex.values().stream(), this.storageIdsBySchemaId::get);

        // Verify that explicit schema model storage ID's agree with their actual assignments
        for (Map.Entry<SchemaId, Schema> entry : schemasBySchemaId.entrySet()) {
            entry.getValue().getSchemaModel().visitSchemaItems(item -> {
                final int modelStorageId = item.getStorageId();
                if (modelStorageId == 0)
                    return;
                final int actualStorageId = this.storageIdsBySchemaId.get(item.getSchemaId());
                if (actualStorageId != modelStorageId) {
                    throw new InconsistentDatabaseException(String.format(
                      "%s in schema \"%s\" has explicit model storage ID %d != assigned storage ID %d",
                      item, entry.getKey(), modelStorageId, actualStorageId));
                }
            });
        }

        // Verify that every storage ID is actually used for something
        for (Map.Entry<Integer, AtomicInteger> entry : refsMap.entrySet()) {
            final int storageId = (int)entry.getKey();
            final AtomicInteger refs = entry.getValue();
            if (refs.get() == 0) {
                final SchemaId schemaId = this.schemaIdsByStorageId.get(storageId);
                throw new InconsistentDatabaseException(String.format(
                  "storage ID %d is assigned to unknown schema ID \"%s\"", storageId, schemaId));
            }
        }

        // Bail out here if no encoding registry
        if (this.encodingRegistry == null)
            return;

        // Initialize schemas (this populates this.schemaItemsBySchemaId as a side effect)
        this.schemasBySchemaIndex.values().forEach(schema -> schema.initialize(this.encodingRegistry));

        // Collect object type storage ID's and build mapping from object type name to storage ID and the inverse
        for (Schema schema : this.schemasBySchemaIndex.values()) {
            for (ObjType objType : schema.getObjTypes().values()) {
                final int storageId = objType.getStorageId();
                final String typeName = objType.getName();
                this.objTypeStorageIds.add(storageId);
                final Integer prevStorageId = this.storageIdsByTypeName.put(typeName, storageId);
                assert prevStorageId == null || (int)prevStorageId == storageId;
                final String prevTypeName = this.typeNamesByStorageId.put(storageId, typeName);
                assert prevTypeName == null || prevTypeName.equals(typeName);
            }
        }

        // Calculate the corresponding KeyRanges (containing all object types)
        this.objTypesKeyRanges = new KeyRanges(this.objTypeStorageIds.stream().map(ObjId::getKeyRange));
    }

// Accessors

    /**
     * Get the encoded form of this instance.
     *
     * @return encoded schema bundle
     */
    public Encoded getEncoded() {
        return new Encoded(this.encoded);
    }

    /**
     * Get the {@link EncodingRegistry} associated with this instance.
     *
     * @return encoding registry
     */
    public EncodingRegistry getEncodingRegistry() {
        return this.encodingRegistry;
    }

    /**
     * Get all of the schemas in this bundle keyed by schema index.
     *
     * @return unmodifiable map of {@link Schema}s indexed by schema index
     */
    public NavigableMap<Integer, Schema> getSchemasBySchemaIndex() {
        return Collections.unmodifiableNavigableMap(this.schemasBySchemaIndex);
    }

    /**
     * Get all of the schemas in this bundle keyed by {@link SchemaId}.
     *
     * @return unmodifiable map of {@link Schema}s indexed by schema ID
     */
    public Map<SchemaId, Schema> getSchemasBySchemaId() {
        return Collections.unmodifiableMap(this.schemasBySchemaId);
    }

    /**
     * Get the {@link Schema} in this bundle with the given schema index.
     *
     * @param schemaIndex schema index
     * @return corresponding schema
     * @throws IllegalArgumentException if {@code schemaIndex} is invalid or unknown
     */
    public Schema getSchema(int schemaIndex) {
        Preconditions.checkArgument(schemaIndex > 0, "invalid schemaIndex");
        final Schema schema = this.schemasBySchemaIndex.get(schemaIndex);
        if (schema == null)
            throw new IllegalArgumentException(String.format("no schema exists with schema %s \"%s\"", "index", schemaIndex));
        return schema;
    }

    /**
     * Get the {@link Schema} in this bundle having the given schema ID.
     *
     * @param schemaId schema structure ID
     * @return schema with ID {@code schemaId}
     * @throws IllegalArgumentException if {@code schemaId} is not known
     */
    public Schema getSchema(SchemaId schemaId) {
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        final Schema schema = this.schemasBySchemaId.get(schemaId);
        if (schema == null)
            throw new IllegalArgumentException(String.format("no schema exists with schema %s \"%s\"", "ID", schemaId));
        return schema;
    }

    /**
     * Get all of the storage ID's in this bundle with their corresponding {@link SchemaId}'s.
     *
     * @return unmodifiable map from storage ID to {@link SchemaId}
     */
    public Map<Integer, SchemaId> getSchemaIdsByStorageId() {
        return Collections.unmodifiableMap(this.schemaIdsByStorageId);
    }

    /**
     * Get the storage ID corresponding to the given schema ID.
     *
     * @param schemaId schema structure ID
     * @return associated storage ID
     * @throws IllegalArgumentException if {@code schemaId} is not known
     */
    public int getStorageId(SchemaId schemaId) {
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        final Integer storageId = this.storageIdsBySchemaId.get(schemaId);
        if (storageId == null)
            throw new IllegalArgumentException(String.format("no storage ID is assigned to schema ID \"%s\"", schemaId));
        return storageId;
    }

    /**
     * Get the object type name corresponding to the given storage ID.
     *
     * @param storageId object type storage ID
     * @return associated object type name
     * @throws IllegalArgumentException if {@code storageId} is invalid or unknown
     */
    public String getObjectTypeName(int storageId) {
        final String name = this.typeNamesByStorageId.get(storageId);
        if (name == null)
            throw new IllegalArgumentException(String.format("no objec type is assigned to storage ID %d", storageId));
        return name;
    }

// Modifications

    /**
     * Build an {@link Encoded} instance with the specified schema added.
     *
     * <p>
     * The schema may already be registered in this bundle. If so, null is returned, reflecting that no change is needed.
     * Otherwise, an encoding of this instance with {@code schemaModel} is returned.
     *
     * <p>
     * In either case, any explicit storage ID assignments in {@code schemaModel} are checked for conflicts with the schemas
     * already registered in this bundle, and any unassigned storage ID's in {@code schemaModel} are automatically assigned.
     *
     * @param schemaIndex schema table index at which to add the schema or zero for next available
     * @param schemaModel the new schema to add
     * @return an encoding of this instance with {@code schemaModel} added, or null if {@code schemaModel} is already registered
     * @throws SchemaMismatchException if {@code schemaModel} has one or more explicit storage ID assignments
     *  and one of them conflicts with a storage ID assignment already registered in this bundle
     * @throws SchemaMismatchException if {@code schemaIndex} is non-zero and some schema already exists at that index
     * @throws IllegalArgumentException if {@code schemaModel} is not locked down
     * @throws IllegalArgumentException if {@code schemaModel} does not validate
     * @throws IllegalArgumentException if {@code schemaModel} is null
     * @throws IllegalArgumentException if {@code schemaIndex} is negative
     */
    public Encoded withSchemaAdded(int schemaIndex, SchemaModel schemaModel) {

        // Sanity check
        Preconditions.checkArgument(schemaModel != null, "null schemaModel");
        Preconditions.checkArgument(schemaModel.isLockedDown(true), "unlocked schemaModel");
        Preconditions.checkArgument(schemaIndex >= 0, "schemaIndex < 0");

        // Validate schema
        schemaModel.validate();
        final SchemaId schemaId = schemaModel.getSchemaId();

        // Debug
        //this.log.info("*** BEFORE ADD SCHEMA \"{}\" @ {}:{}", schemaId, schemaIndex, "\n" + this.encoded);

        // Copy this instance's encoded tables
        final TreeMap<Integer, ByteData> schemaBytes = new TreeMap<>(this.encoded.getSchemaBytes());
        final TreeMap<Integer, ByteData> storageIdBytes = new TreeMap<>(this.encoded.getStorageIdBytes());
        final HashMap<SchemaId, Integer> newStorageIdsBySchemaId = new HashMap<>(this.storageIdsBySchemaId);

        // Track whether anything actually changes
        final AtomicBoolean bundleChanged = new AtomicBoolean();

        // If schema is not already registered, add it to the schema table at the next available (or specified) index
        final boolean schemaIsNew = !this.schemasBySchemaId.containsKey(schemaId);
        if (schemaIsNew) {

            // Encode schema model
            final ByteData.Writer writer = ByteData.newWriter();
            Layout.encodeSchema(writer, schemaModel);
            final ByteData encodedSchema = writer.toByteData();

            // Add to schema table
            if (schemaIndex == 0)
                this.assignNextFreeIndex(schemaBytes, encodedSchema);
            else if (schemaBytes.put(schemaIndex, encodedSchema) != null)
                throw new SchemaMismatchException(schemaId, String.format("schema index %d is already in use", schemaIndex));

            // Flag the change
            bundleChanged.set(true);
        }

        // Sort the new schema's schema items so those having explicit storage ID's appear first
        final ArrayList<io.permazen.schema.SchemaItem> schemaItemList = new ArrayList<>();
        schemaModel.visitSchemaItems(schemaItemList::add);
        schemaItemList.sort(Comparator.comparing(io.permazen.schema.SchemaItem::getStorageId).reversed());

        // Verify explicit storage ID assignements and/or make new ones as needed
        schemaItemList.forEach(item -> {

            // Get schema ID and explicit storage ID assignment, if any
            final SchemaId itemSchemaId = item.getSchemaId();
            final int modelStorageId = item.getStorageId();

            // Does this item already have an assignment in this bundle? If so, any explicit storage ID must agree
            final Integer actualStorageIdObj = newStorageIdsBySchemaId.get(itemSchemaId);
            if (actualStorageIdObj != null) {
                final int actualStorageId = (int)actualStorageIdObj;
                if (modelStorageId != 0 && actualStorageId != modelStorageId) {
                    throw new SchemaMismatchException(schemaId, String.format(
                      "%s in schema \"%s\" has explicit model storage ID %d != storage ID %d already assigned in the database",
                      item, schemaId, modelStorageId, actualStorageId));
                }
                return;         // no new storage ID assignment needed
            }

            // Encode this item's schema ID
            final ByteData.Writer idWriter = ByteData.newWriter();
            Encodings.STRING.write(idWriter, itemSchemaId.getId());
            final ByteData encodedItemSchemaId = idWriter.toByteData();

            // If item has an explicit storage ID, check it doesn't conflict with any existing assignments and assign it
            if (modelStorageId != 0) {
                final ByteData prevSchemaIdBytes = storageIdBytes.put(modelStorageId, encodedItemSchemaId);
                if (prevSchemaIdBytes != null && !prevSchemaIdBytes.equals(encodedItemSchemaId)) {

                    // Decode the conflicting schema ID
                    final SchemaId prevSchemaId = new SchemaId(Encodings.STRING.read(prevSchemaIdBytes.newReader()));

                    // Get a random example schema item that is using this schema ID
                    final SchemaItem prevItem = this.schemaItemsBySchemaId.get(prevSchemaId);
                    final String conflictor = prevItem != null ?
                      String.format("(for example) %s", prevItem) :
                      "an unknown conflicting schema item";

                    // Throw exception
                    throw new SchemaMismatchException(schemaId, String.format(
                      "%s in schema \"%s\" has explicit model storage ID %d already assigned in the database to \"%s\" used by %s",
                      item, schemaId, modelStorageId, prevSchemaId, conflictor));
                }
                newStorageIdsBySchemaId.put(itemSchemaId, modelStorageId);
            } else {

                // Otherwise, assign the next available storage ID
                final int storageId = this.assignNextFreeIndex(storageIdBytes, encodedItemSchemaId);
                newStorageIdsBySchemaId.put(itemSchemaId, storageId);
            }

            // Flag the change
            bundleChanged.set(true);
        });

        // Return new schema bundle in encoded form
        final Encoded result = bundleChanged.get() ? new Encoded(schemaBytes, storageIdBytes) : null;
        //this.log.info("*** AFTER ADD SCHEMA \"{}\" @ {}:{}", schemaId, schemaIndex, result != null ? "\n" + result : " Same");
        return result;
    }

    /**
     * Build an {@link Encoded} instance with the specified schema removed.
     *
     * <p>
     * Any obsolete storage ID assignments will be removed automatically.
     *
     * @param schemaId ID of the schema to remove
     * @throws IllegalArgumentException if {@code schemaId} is null
     * @throws IllegalArgumentException if the schema does not exist
     */
    public Encoded withSchemaRemoved(SchemaId schemaId) {

        // Sanity check
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        final Schema schema = this.schemasBySchemaId.get(schemaId);
        Preconditions.checkArgument(schema != null, "schema not found");

        // Copy this instance's encoded tables
        final TreeMap<Integer, ByteData> schemaBytes = new TreeMap<>(this.encoded.getSchemaBytes());
        final TreeMap<Integer, ByteData> storageIdBytes = new TreeMap<>(this.encoded.getStorageIdBytes());

        // Get schema's table index
        final int schemaIndex = schema.getSchemaIndex();
        assert this.schemasBySchemaIndex.get(schemaIndex) == schema;

        // Remove schema from encoded schema table
        //this.log.info("*** BEFORE REMOVE SCHEMA \"{}\" @ {}\n{}", schemaId, schemaIndex, this.encoded);
        schemaBytes.remove(schemaIndex);

        // Remove storage ID assignments we no longer need
        final TreeMap<Integer, AtomicInteger> refs = SchemaBundle.countStorageIdRefs(
          this.schemaIdsByStorageId.keySet(), this.schemasBySchemaIndex.values().stream().filter(s -> s != schema),
          this.storageIdsBySchemaId::get);
        for (Map.Entry<Integer, AtomicInteger> entry : refs.entrySet()) {
            if (entry.getValue().get() == 0)
                storageIdBytes.remove(entry.getKey());
        }

        // Return new schema bundle in encoded form
        final Encoded newEncoded = new Encoded(schemaBytes, storageIdBytes);
        //this.log.info("*** AFTER REMOVE SCHEMA \"{}\" @ {}\n{}", schemaId, schemaIndex, newEncoded);
        return newEncoded;
    }

    // Find the first available index > 0 and assign the given item. This assumes the lowest possible existing index is 1.
    private <V> int assignNextFreeIndex(NavigableMap<Integer, V> map, V item) {

        // Get key set
        final NavigableSet<Integer> indexes = map.navigableKeySet();

        // Find the next available schema index (this is linear time - could be faster with a binary search)
        int candidate = 1;
        for (int next : map.navigableKeySet()) {

            // Did we find a hole?
            if (next > candidate)
                break;
            assert next == candidate;

            // Optimize for the fully contigous case (first iteration only)
            if (candidate == 1 && indexes.last() == indexes.size()) {
                candidate = indexes.size() + 1;
                break;
            }

            // Try the next slot
            candidate++;
        }

        // Add new item
        map.put(candidate, item);
        return candidate;
    }

// Package Methods

    Encoded getEncodedNoCopy() {
        return this.encoded;
    }

    HashMap<String, Integer> getStorageIdsByTypeName() {
        return this.storageIdsByTypeName;
    }

    HashMap<Integer, String> getTypeNamesByStorageId() {
        return this.typeNamesByStorageId;
    }

    TreeSet<Integer> getObjTypeStorageIds() {
        return this.objTypeStorageIds;
    }

    String getTypeName(int storageId) {
        return this.typeNamesByStorageId.get(storageId);
    }

    KeyRanges getObjTypesKeyRanges() {
        return this.objTypesKeyRanges;
    }

    /**
     * Record a representative {@link SchemaItem} corresponding to the given {@link SchemaId}.
     *
     * @param item schema item
     * @throws IllegalArgumentException if {@code item} is null
     */
    void registerSchemaItemForSchemaId(SchemaItem item) {
        this.schemaItemsBySchemaId.put(item.getSchemaId(), item);
    }

    /**
     * Get <b>a random representative</b> {@link SchemaItem} corresponding to the given {@link SchemaId}.
     *
     * <p>
     * Because the representative is randomly chosen from all schemas, {@link SchemaItem#getSchema} should be ignored.
     *
     * @param schemaId schema ID
     * @param expectedType expected schema item type
     * @return {@link SchemaItem} representative
     * @throws UnknownFieldException if a {@link Field} schema ID is not found
     * @throws UnknownTypeException if an {@link ObjType} schema ID is not found
     * @throws UnknownIndexException if an {@link Index} schema ID is not found
     * @throws IllegalArgumentException if {@code schemaId} or {@code expectedType} is invalid
     * @throws IllegalArgumentException if {@code schemaId} is null
     */
    <T extends SchemaItem> T getSchemaItem(SchemaId schemaId, Class<T> expectedType) {

        // Sanity check
        Preconditions.checkArgument(schemaId != null, "null schemaId");
        Preconditions.checkArgument(expectedType != null, "null expectedType");

        // Lookup item
        final SchemaItem schemaItem = this.schemaItemsBySchemaId.get(schemaId);
        if (expectedType.isInstance(schemaItem))
            return expectedType.cast(schemaItem);

        // Throw an appropriate exception
        String message = String.format("no %s with schema ID \"%s\" found", SchemaBundle.getDescription(expectedType), schemaId);
        if (schemaItem != null)
            message += " (found " + schemaItem + " instead)";
        throw this.createNotFoundException(expectedType, schemaId.toString(), message);
    }

    /**
     * Same thing but lookup using storage ID instead of Schema ID.
     */
    <T extends SchemaItem> T getSchemaItem(int storageId, Class<T> expectedType) {

        // Sanity check
        Preconditions.checkArgument(expectedType != null, "null expectedType");

        // Lookup schema ID
        final SchemaId schemaId = this.schemaIdsByStorageId.get(storageId);
        if (schemaId == null) {
            String message = String.format("no %s with storage ID %d found", SchemaBundle.getDescription(expectedType), storageId);
            throw this.createNotFoundException(expectedType, String.format("storage ID %d", storageId), message);
        }
        return this.getSchemaItem(schemaId, expectedType);
    }

    private RuntimeException createNotFoundException(Class<? extends SchemaItem> expectedType, String name, String message) {
        if (Field.class.isAssignableFrom(expectedType))
            throw new UnknownFieldException(name, message);
        if (ObjType.class.isAssignableFrom(expectedType))
            throw new UnknownTypeException(name, null, message);
        if (Index.class.isAssignableFrom(expectedType))
            throw new UnknownIndexException(name, message);
        throw new IllegalArgumentException(message);                        // should never get here
    }

    static String getDescription(Class<? extends SchemaItem> type) {
        if (ObjType.class.isAssignableFrom(type))
            return "object type";
        return type.getSimpleName()
          .replaceAll("([a-z])([A-Z])", "$1 $2")
          .toLowerCase();
    }

    boolean matches(SchemaBundle that) {
        Preconditions.checkArgument(that != null, "null that");
        return this.encoded.equals(that.encoded);
    }

// Internal Methods

    // Given a set of storage ID's, a stream of SchemaModels, and an assignment function from SchemaId to storage ID,
    // build a mappging from Storage ID to the number of schema items assigned to it (via their SchemaId's).
    // If we encounter a SchemaID that does not have a storage ID assignment, throw an exception.
    private static TreeMap<Integer, AtomicInteger> countStorageIdRefs(
      Iterable<Integer> storageIds, Stream<Schema> schemas, Function<SchemaId, Integer> assignment) {

        // Initialize mapping
        final TreeMap<Integer, AtomicInteger> refsMap = new TreeMap<>();       // storage ID -> #refs
        for (int storageId : storageIds)
            refsMap.put(storageId, new AtomicInteger());

        // Tally references
        SchemaBundle.visitSchemaItems(schemas.map(Schema::getSchemaModel), item -> {
            final SchemaId schemaId = item.getSchemaId();
            final Integer storageId = assignment.apply(schemaId);
            if (storageId == null) {
                throw new InconsistentDatabaseException(String.format(
                  "no storage ID assigned to schema ID \"%s\" (%s)", schemaId, item));
            }
            final AtomicInteger refs = refsMap.get(storageId);
            Preconditions.checkArgument(refs != null, "internal error");
            refs.incrementAndGet();
        });

        // Done
        return refsMap;
    }

    // Iterate over all (model) schema items that require storage ID's
    private static void visitSchemaItems(Stream<SchemaModel> schemaModels, Consumer<io.permazen.schema.SchemaItem> visitor) {
        schemaModels.forEach(schemaModel -> schemaModel.visitSchemaItems(visitor));
    }

// Encoding

    /**
     * The {@code byte[]}-encoded version of a {@link SchemaBundle} that is actually stored in a {@link Database}.
     */
    public static class Encoded {

        private final ImmutableNavigableMap<Integer, ByteData> schemaBytes;
        private final ImmutableNavigableMap<Integer, ByteData> storageIdBytes;

    // Constructors

        /**
         * Constructor.
         *
         * @param schemaBytes encoded schema table
         * @param storageIdBytes encoded storage ID table
         * @throws IllegalArgumentException if either parameter is null
         */
        public Encoded(NavigableMap<Integer, ByteData> schemaBytes, NavigableMap<Integer, ByteData> storageIdBytes) {

            // Sanity check
            Preconditions.checkArgument(schemaBytes != null, "null schemaBytes");
            Preconditions.checkArgument(storageIdBytes != null, "null storageIdBytes");

            // Initialize
            this.schemaBytes = new ImmutableNavigableMap<>(schemaBytes);
            this.storageIdBytes = new ImmutableNavigableMap<>(storageIdBytes);
        }

        /**
         * Copy constructor.
         *
         * @param original instance to copy
         * @throws IllegalArgumentException if {@code original} is null
         */
        public Encoded(Encoded original) {
            Preconditions.checkArgument(original != null, "null original");
            this.schemaBytes = new ImmutableNavigableMap<>(original.schemaBytes);
            this.storageIdBytes = new ImmutableNavigableMap<>(original.storageIdBytes);
        }

    // Accessors

        /**
         * Get the encoded schema table.
         *
         * @return encoded table mapping schema index to {@link SchemaModel}
         */
        public NavigableMap<Integer, ByteData> getSchemaBytes() {
            return this.schemaBytes;
        }

        /**
         * Get the encoded storage ID table.
         *
         * @return encoded table mapping storage ID to {@link SchemaId}
         */
        public NavigableMap<Integer, ByteData> getStorageIdBytes() {
            return this.storageIdBytes;
        }

    // Reading/Writing

        /**
         * Read encoded {@link SchemaBundle} data from the given key/value store.
         *
         * @param kv key/value store
         * @throws IllegalArgumentException if {@code kv} is null
         * @throws InconsistentDatabaseException if data is invalid
         */
        public static Encoded readFrom(KVStore kv) {

            // Sanity check
            Preconditions.checkArgument(kv != null, "null kv");

            // Initialize
            final TreeMap<Integer, ByteData> schemaBytes = new TreeMap<>();
            final TreeMap<Integer, ByteData> storageIdBytes = new TreeMap<>();

            // Read tables
            final ByteData schemaPrefix = Layout.getSchemaTablePrefix();
            final ByteData storageIdPrefix = Layout.getStorageIdTablePrefix();
            final ByteData endOfStorageIdTable = ByteUtil.getKeyAfterPrefix(storageIdPrefix);
            try (CloseableIterator<KVPair> i = kv.getRange(schemaPrefix, endOfStorageIdTable)) {
                while (i.hasNext()) {
                    final KVPair pair = i.next();
                    final ByteData key = pair.getKey();

                    // Which table are we reading now?
                    final boolean storageIds = key.startsWith(storageIdPrefix);

                    // Parse this entry's index (schema index or storage ID)
                    final ByteData.Reader reader = key.newReader(storageIds ? storageIdPrefix.size() : schemaPrefix.size());
                    final int index = UnsignedIntEncoder.read(reader);
                    if (index <= 0) {
                        throw new InconsistentDatabaseException(String.format(
                          "invalid %s table index %d at key %s",
                          storageIds ? "storage ID" : "schema", index, ByteUtil.toString(key)));
                    }
                    if (reader.remain() > 0) {
                        throw new InconsistentDatabaseException(String.format(
                          "%s table key %s (index %d) contains trailing garbage",
                          storageIds ? "storage ID" : "schema", ByteUtil.toString(key), index));
                    }

                    // Add byte[] array to map
                    (storageIds ? storageIdBytes : schemaBytes).put(index, pair.getValue());
                }
            }

            // Done
            return new Encoded(schemaBytes, storageIdBytes);
        }

        /**
         * Rewrite the schema and storage ID tables in the given key/value store with the contents of this instance.
         *
         * @param kv key/value store
         * @throws IllegalArgumentException if {@code kv} is null
         */
        public void writeTo(KVStore kv) {
            Preconditions.checkArgument(kv != null, "null kv");
            kv.removeRange(Layout.getSchemaTablePrefix(), ByteUtil.getKeyAfterPrefix(Layout.getStorageIdTablePrefix()));
            this.writeTable(kv, Layout.getSchemaTablePrefix(), this.schemaBytes);
            this.writeTable(kv, Layout.getStorageIdTablePrefix(), this.storageIdBytes);
        }

        private void writeTable(KVStore kv, ByteData prefix, NavigableMap<Integer, ByteData> table) {
            table.forEach((index, bytes) -> kv.put(Layout.buildTableKey(prefix, index), bytes));
        }

    // Private Methods

        /**
         * Populate the given {@link SchemaBundle}'s Storage ID Table, which must be empty, from this instance.
         *
         * @throws IllegalArgumentException if this instance contains bad data
         */
        private void decodeStorageIdTable(SchemaBundle schemaBundle) {

            // Sanity check
            assert schemaBundle != null;
            assert schemaBundle.schemaIdsByStorageId.isEmpty();

            // Decode the Storage ID Table
            for (Map.Entry<Integer, ByteData> entry : storageIdBytes.entrySet()) {
                final int storageId = entry.getKey();
                final ByteData.Reader reader = entry.getValue().newReader();
                assert storageId > 0;

                // Decode schema ID
                final SchemaId schemaId;
                try {
                    final String string = Encodings.STRING.read(reader);
                    if (reader.remain() > 0)
                        throw new IllegalArgumentException("entry contains trailing garbage");
                    schemaId = new SchemaId(string);
                } catch (IndexOutOfBoundsException | IllegalArgumentException e) {
                    throw new IllegalArgumentException(String.format(
                      "invalid schema ID for storage ID %d: %s", storageId, e.getMessage()), e);
                }

                // Record storage ID
                schemaBundle.schemaIdsByStorageId.put(storageId, schemaId);
            }
        }

        /**
         * Populate the given {@link SchemaBundle}'s Schema Table, which must be empty, from this instance.
         *
         * @throws IllegalArgumentException if this instance contains bad data
         */
        private void decodeSchemaTable(SchemaBundle schemaBundle) {

            // Sanity check
            assert schemaBundle != null;
            assert schemaBundle.schemasBySchemaIndex.isEmpty();

            // Decode the Schema Table
            for (Map.Entry<Integer, ByteData> entry : this.schemaBytes.entrySet()) {
                final int schemaIndex = entry.getKey();
                final ByteData schemaData = entry.getValue();
                assert schemaIndex > 0;

                // Decode schema model
                final SchemaModel schemaModel;
                try {
                    schemaModel = Layout.decodeSchema(schemaData);
                } catch (InvalidSchemaException e) {
                    throw new IllegalArgumentException(String.format(
                      "invalid encoded schema at schema index %d: %s", schemaIndex, e.getMessage()), e);
                }

                // Validate schema model
                try {
                    schemaModel.lockDown(true);
                    schemaModel.validate();
                } catch (InvalidSchemaException e) {
                    throw new IllegalArgumentException(String.format(
                      "invalid schema at schema index %d: %s", schemaIndex, e.getMessage()), e);
                }

                // Record schema
                schemaBundle.schemasBySchemaIndex.put(schemaIndex, new Schema(schemaBundle, schemaIndex, schemaModel));
            }
        }

    // Object

        @Override
        public int hashCode() {
            return this.schemaBytes.hashCode() ^ this.storageIdBytes.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final Encoded that = (Encoded)obj;
            return this.schemaBytes.equals(that.schemaBytes)
              && this.storageIdBytes.equals(that.storageIdBytes);
        }

        // For debugging
        @Override
        public String toString() {
            final StringBuilder buf = new StringBuilder();
            for (int i = 0; i < 2; i++) {
                final boolean schemaTable = i == 0;
                final String label = schemaTable ? "Schema Table" : "Storage ID Table";
                final NavigableMap<Integer, ByteData> map = schemaTable ? schemaBytes : storageIdBytes;
                buf.append(label).append(':');
                map.forEach((index, data) -> {
                    buf.append(String.format("%n%d: %s", index, ByteUtil.toString(data)));
                    if (!schemaTable) {           // decode the SchemaId
                        try {
                            buf.append(String.format(" (%s)",
                              Encodings.STRING.toString(Encodings.STRING.read(data.newReader()))));
                        } catch (IllegalArgumentException e) {
                            buf.append(String.format(" (can't decode: %s)", e));
                        }
                    }
                });
                if (schemaTable)
                    buf.append('\n');
            }
            return buf.toString();
        }
    }
}
