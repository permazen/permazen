
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.fdb;

import org.dellroad.stuff.util.LongMap;

/**
 * FoundationDB error codes.
 *
 * @see <a href="https://github.com/apple/foundationdb/blob/main/flow/include/flow/error_definitions.h">Error Codes</a>
 */
public enum ErrorCode {

    //
    // Generated with this shell command:
    //
    //    $ grep ^ERROR ./flow/error_definitions.h \
    //        | cut -c8- \
    //        | sed -E 's/^([^,]+),[[:space:]]*([^,]+),[[:space:]]+("[^"]+").*$/\1@\2@\3/g' \
    //        | (     IFS=@;
    //                while read i n s; do
    //                    printf '    /**\n     * %s.\n     */\n    %s(%d, %s),\n\n' \
    //                        "`echo $s | tr -d \\\"`" \
    //                        "`echo $i | tr [a-z] [A-Z]`" \
    //                        "$n" "$s"
    //                    done
    //          )

    /**
     * Success.
     */
    SUCCESS(0, "Success"),

    /**
     * End of stream.
     */
    END_OF_STREAM(1, "End of stream"),

    /**
     * Operation failed.
     */
    OPERATION_FAILED(1000, "Operation failed"),

    /**
     * Shard is not available from this server.
     */
    WRONG_SHARD_SERVER(1001, "Shard is not available from this server"),

    /**
     * Operation timed out.
     */
    TIMED_OUT(1004, "Operation timed out"),

    /**
     * Conflict occurred while changing coordination information.
     */
    COORDINATED_STATE_CONFLICT(1005, "Conflict occurred while changing coordination information"),

    /**
     * All alternatives failed.
     */
    ALL_ALTERNATIVES_FAILED(1006, "All alternatives failed"),

    /**
     * Transaction is too old to perform reads or be committed.
     */
    TRANSACTION_TOO_OLD(1007, "Transaction is too old to perform reads or be committed"),

    /**
     * Not enough physical servers available.
     */
    NO_MORE_SERVERS(1008, "Not enough physical servers available"),

    /**
     * Request for future version.
     */
    FUTURE_VERSION(1009, "Request for future version"),

    /**
     * Conflicting attempts to change data distribution.
     */
    MOVEKEYS_CONFLICT(1010, "Conflicting attempts to change data distribution"),

    /**
     * TLog stopped.
     */
    TLOG_STOPPED(1011, "TLog stopped"),

    /**
     * Server request queue is full.
     */
    SERVER_REQUEST_QUEUE_FULL(1012, "Server request queue is full"),

    /**
     * Transaction not committed due to conflict with another transaction.
     */
    NOT_COMMITTED(1020, "Transaction not committed due to conflict with another transaction"),

    /**
     * Transaction may or may not have committed.
     */
    COMMIT_UNKNOWN_RESULT(1021, "Transaction may or may not have committed"),

    /**
     * Operation aborted because the transaction was cancelled.
     */
    TRANSACTION_CANCELLED(1025, "Operation aborted because the transaction was cancelled"),

    /**
     * Network connection failed.
     */
    CONNECTION_FAILED(1026, "Network connection failed"),

    /**
     * Coordination servers have changed.
     */
    COORDINATORS_CHANGED(1027, "Coordination servers have changed"),

    /**
     * New coordination servers did not respond in a timely way.
     */
    NEW_COORDINATORS_TIMED_OUT(1028, "New coordination servers did not respond in a timely way"),

    /**
     * Watch cancelled because storage server watch limit exceeded.
     */
    WATCH_CANCELLED(1029, "Watch cancelled because storage server watch limit exceeded"),

    /**
     * Request may or may not have been delivered.
     */
    REQUEST_MAYBE_DELIVERED(1030, "Request may or may not have been delivered"),

    /**
     * Operation aborted because the transaction timed out.
     */
    TRANSACTION_TIMED_OUT(1031, "Operation aborted because the transaction timed out"),

    /**
     * Too many watches currently set.
     */
    TOO_MANY_WATCHES(1032, "Too many watches currently set"),

    /**
     * Locality information not available.
     */
    LOCALITY_INFORMATION_UNAVAILABLE(1033, "Locality information not available"),

    /**
     * Watches cannot be set if read your writes is disabled.
     */
    WATCHES_DISABLED(1034, "Watches cannot be set if read your writes is disabled"),

    /**
     * Default error for an ErrorOr object.
     */
    DEFAULT_ERROR_OR(1035, "Default error for an ErrorOr object"),

    /**
     * Read or wrote an unreadable key.
     */
    ACCESSED_UNREADABLE(1036, "Read or wrote an unreadable key"),

    /**
     * Storage process does not have recent mutations.
     */
    PROCESS_BEHIND(1037, "Storage process does not have recent mutations"),

    /**
     * Database is locked.
     */
    DATABASE_LOCKED(1038, "Database is locked"),

    /**
     * The protocol version of the cluster has changed.
     */
    CLUSTER_VERSION_CHANGED(1039, "The protocol version of the cluster has changed"),

    /**
     * External client has already been loaded.
     */
    EXTERNAL_CLIENT_ALREADY_LOADED(1040, "External client has already been loaded"),

    /**
     * DNS lookup failed.
     */
    LOOKUP_FAILED(1041, "DNS lookup failed"),

    /**
     * Broken promise.
     */
    BROKEN_PROMISE(1100, "Broken promise"),

    /**
     * Asynchronous operation cancelled.
     */
    OPERATION_CANCELLED(1101, "Asynchronous operation cancelled"),

    /**
     * Future has been released.
     */
    FUTURE_RELEASED(1102, "Future has been released"),

    /**
     * Connection object leaked.
     */
    CONNECTION_LEAKED(1103, "Connection object leaked"),

    /**
     * Recruitment of a server failed.
     */
    RECRUITMENT_FAILED(1200, "Recruitment of a server failed"),

    /**
     * Attempt to move keys to a storage server that was removed.
     */
    MOVE_TO_REMOVED_SERVER(1201, "Attempt to move keys to a storage server that was removed"),

    /**
     * Normal worker shut down.
     */
    WORKER_REMOVED(1202, "Normal worker shut down"),

    /**
     * Master recovery failed.
     */
    MASTER_RECOVERY_FAILED(1203, "Master recovery failed"),

    /**
     * Master hit maximum number of versions in flight.
     */
    MASTER_MAX_VERSIONS_IN_FLIGHT(1204, "Master hit maximum number of versions in flight"),

    /**
     * Master terminating because a TLog failed.
     */
    MASTER_TLOG_FAILED(1205, "Master terminating because a TLog failed"),

    /**
     * Recovery of a worker process failed.
     */
    WORKER_RECOVERY_FAILED(1206, "Recovery of a worker process failed"),

    /**
     * Reboot of server process requested.
     */
    PLEASE_REBOOT(1207, "Reboot of server process requested"),

    /**
     * Reboot of server process requested, with deletion of state.
     */
    PLEASE_REBOOT_DELETE(1208, "Reboot of server process requested, with deletion of state"),

    /**
     * Master terminating because a Proxy failed.
     */
    MASTER_PROXY_FAILED(1209, "Master terminating because a Proxy failed"),

    /**
     * Master terminating because a Resolver failed.
     */
    MASTER_RESOLVER_FAILED(1210, "Master terminating because a Resolver failed"),

    /**
     * Platform error.
     */
    PLATFORM_ERROR(1500, "Platform error"),

    /**
     * Large block allocation failed.
     */
    LARGE_ALLOC_FAILED(1501, "Large block allocation failed"),

    /**
     * QueryPerformanceCounter error.
     */
    PERFORMANCE_COUNTER_ERROR(1502, "QueryPerformanceCounter error"),

    /**
     * Disk i/o operation failed.
     */
    IO_ERROR(1510, "Disk i/o operation failed"),

    /**
     * File not found.
     */
    FILE_NOT_FOUND(1511, "File not found"),

    /**
     * Unable to bind to network.
     */
    BIND_FAILED(1512, "Unable to bind to network"),

    /**
     * File could not be read.
     */
    FILE_NOT_READABLE(1513, "File could not be read"),

    /**
     * File could not be written.
     */
    FILE_NOT_WRITABLE(1514, "File could not be written"),

    /**
     * No cluster file found in current directory or default location.
     */
    NO_CLUSTER_FILE_FOUND(1515, "No cluster file found in current directory or default location"),

    /**
     * File too large to be read.
     */
    FILE_TOO_LARGE(1516, "File too large to be read"),

    /**
     * Non sequential file operation not allowed.
     */
    NON_SEQUENTIAL_OP(1517, "Non sequential file operation not allowed"),

    /**
     * HTTP response was badly formed.
     */
    HTTP_BAD_RESPONSE(1518, "HTTP response was badly formed"),

    /**
     * HTTP request not accepted.
     */
    HTTP_NOT_ACCEPTED(1519, "HTTP request not accepted"),

    /**
     * A data checksum failed.
     */
    CHECKSUM_FAILED(1520, "A data checksum failed"),

    /**
     * A disk IO operation failed to complete in a timely manner.
     */
    IO_TIMEOUT(1521, "A disk IO operation failed to complete in a timely manner"),

    /**
     * A structurally corrupt data file was detected.
     */
    FILE_CORRUPT(1522, "A structurally corrupt data file was detected"),

    /**
     * HTTP response code indicated failure.
     */
    HTTP_REQUEST_FAILED(1523, "HTTP response code indicated failure"),

    /**
     * HTTP request failed due to bad credentials.
     */
    HTTP_AUTH_FAILED(1524, "HTTP request failed due to bad credentials"),

    /**
     * Invalid API call.
     */
    CLIENT_INVALID_OPERATION(2000, "Invalid API call"),

    /**
     * Commit with incomplete read.
     */
    COMMIT_READ_INCOMPLETE(2002, "Commit with incomplete read"),

    /**
     * Invalid test specification.
     */
    TEST_SPECIFICATION_INVALID(2003, "Invalid test specification"),

    /**
     * Key outside legal range.
     */
    KEY_OUTSIDE_LEGAL_RANGE(2004, "Key outside legal range"),

    /**
     * Range begin key larger than end key.
     */
    INVERTED_RANGE(2005, "Range begin key larger than end key"),

    /**
     * Option set with an invalid value.
     */
    INVALID_OPTION_VALUE(2006, "Option set with an invalid value"),

    /**
     * Option not valid in this context.
     */
    INVALID_OPTION(2007, "Option not valid in this context"),

    /**
     * Action not possible before the network is configured.
     */
    NETWORK_NOT_SETUP(2008, "Action not possible before the network is configured"),

    /**
     * Network can be configured only once.
     */
    NETWORK_ALREADY_SETUP(2009, "Network can be configured only once"),

    /**
     * Transaction already has a read version set.
     */
    READ_VERSION_ALREADY_SET(2010, "Transaction already has a read version set"),

    /**
     * Version not valid.
     */
    VERSION_INVALID(2011, "Version not valid"),

    /**
     * Range limits not valid.
     */
    RANGE_LIMITS_INVALID(2012, "Range limits not valid"),

    /**
     * Database name must be 'DB'.
     */
    INVALID_DATABASE_NAME(2013, "Database name must be 'DB'"),

    /**
     * Attribute not found in string.
     */
    ATTRIBUTE_NOT_FOUND(2014, "Attribute not found in string"),

    /**
     * Future not ready.
     */
    FUTURE_NOT_SET(2015, "Future not ready"),

    /**
     * Future not an error.
     */
    FUTURE_NOT_ERROR(2016, "Future not an error"),

    /**
     * Operation issued while a commit was outstanding.
     */
    USED_DURING_COMMIT(2017, "Operation issued while a commit was outstanding"),

    /**
     * Unrecognized atomic mutation type.
     */
    INVALID_MUTATION_TYPE(2018, "Unrecognized atomic mutation type"),

    /**
     * Attribute too large for type int.
     */
    ATTRIBUTE_TOO_LARGE(2019, "Attribute too large for type int"),

    /**
     * Transaction does not have a valid commit version.
     */
    TRANSACTION_INVALID_VERSION(2020, "Transaction does not have a valid commit version"),

    /**
     * Transaction is read-only and therefore does not have a commit version.
     */
    NO_COMMIT_VERSION(2021, "Transaction is read-only and therefore does not have a commit version"),

    /**
     * Environment variable network option could not be set.
     */
    ENVIRONMENT_VARIABLE_NETWORK_OPTION_FAILED(2022, "Environment variable network option could not be set"),

    /**
     * Attempted to commit a transaction specified as read-only.
     */
    TRANSACTION_READ_ONLY(2023, "Attempted to commit a transaction specified as read-only"),

    /**
     * Incompatible protocol version.
     */
    INCOMPATIBLE_PROTOCOL_VERSION(2100, "Incompatible protocol version"),

    /**
     * Transaction exceeds byte limit.
     */
    TRANSACTION_TOO_LARGE(2101, "Transaction exceeds byte limit"),

    /**
     * Key length exceeds limit.
     */
    KEY_TOO_LARGE(2102, "Key length exceeds limit"),

    /**
     * Value length exceeds limit.
     */
    VALUE_TOO_LARGE(2103, "Value length exceeds limit"),

    /**
     * Connection string invalid.
     */
    CONNECTION_STRING_INVALID(2104, "Connection string invalid"),

    /**
     * Local address in use.
     */
    ADDRESS_IN_USE(2105, "Local address in use"),

    /**
     * Invalid local address.
     */
    INVALID_LOCAL_ADDRESS(2106, "Invalid local address"),

    /**
     * TLS error.
     */
    TLS_ERROR(2107, "TLS error"),

    /**
     * Operation is not supported.
     */
    UNSUPPORTED_OPERATION(2108, "Operation is not supported"),

    /**
     * API version is not set.
     */
    API_VERSION_UNSET(2200, "API version is not set"),

    /**
     * API version may be set only once.
     */
    API_VERSION_ALREADY_SET(2201, "API version may be set only once"),

    /**
     * API version not valid.
     */
    API_VERSION_INVALID(2202, "API version not valid"),

    /**
     * API version not supported.
     */
    API_VERSION_NOT_SUPPORTED(2203, "API version not supported"),

    /**
     * EXACT streaming mode requires limits, but none were given.
     */
    EXACT_MODE_WITHOUT_LIMITS(2210, "EXACT streaming mode requires limits, but none were given"),

    /**
     * Unrecognized data type in packed tuple.
     */
    INVALID_TUPLE_DATA_TYPE(2250, "Unrecognized data type in packed tuple"),

    /**
     * Tuple does not have element at specified index.
     */
    INVALID_TUPLE_INDEX(2251, "Tuple does not have element at specified index"),

    /**
     * Cannot unpack key that is not in subspace.
     */
    KEY_NOT_IN_SUBSPACE(2252, "Cannot unpack key that is not in subspace"),

    /**
     * Cannot specify a prefix unless manual prefixes are enabled.
     */
    MANUAL_PREFIXES_NOT_ENABLED(2253, "Cannot specify a prefix unless manual prefixes are enabled"),

    /**
     * Cannot specify a prefix in a partition.
     */
    PREFIX_IN_PARTITION(2254, "Cannot specify a prefix in a partition"),

    /**
     * Root directory cannot be opened.
     */
    CANNOT_OPEN_ROOT_DIRECTORY(2255, "Root directory cannot be opened"),

    /**
     * Directory already exists.
     */
    DIRECTORY_ALREADY_EXISTS(2256, "Directory already exists"),

    /**
     * Directory does not exist.
     */
    DIRECTORY_DOES_NOT_EXIST(2257, "Directory does not exist"),

    /**
     * Directory's parent does not exist.
     */
    PARENT_DIRECTORY_DOES_NOT_EXIST(2258, "Directory's parent does not exist"),

    /**
     * Directory has already been created with a different layer string.
     */
    MISMATCHED_LAYER(2259, "Directory has already been created with a different layer string"),

    /**
     * Invalid directory layer metadata.
     */
    INVALID_DIRECTORY_LAYER_METADATA(2260, "Invalid directory layer metadata"),

    /**
     * Directory cannot be moved between partitions.
     */
    CANNOT_MOVE_DIRECTORY_BETWEEN_PARTITIONS(2261, "Directory cannot be moved between partitions"),

    /**
     * Directory partition cannot be used as subspace.
     */
    CANNOT_USE_PARTITION_AS_SUBSPACE(2262, "Directory partition cannot be used as subspace"),

    /**
     * Directory layer was created with an incompatible version.
     */
    INCOMPATIBLE_DIRECTORY_VERSION(2263, "Directory layer was created with an incompatible version"),

    /**
     * Database has keys stored at the prefix chosen by the automatic prefix allocator.
     */
    DIRECTORY_PREFIX_NOT_EMPTY(2264, "Database has keys stored at the prefix chosen by the automatic prefix allocator"),

    /**
     * Directory layer already has a conflicting prefix.
     */
    DIRECTORY_PREFIX_IN_USE(2265, "Directory layer already has a conflicting prefix"),

    /**
     * Target directory is invalid.
     */
    INVALID_DESTINATION_DIRECTORY(2266, "Target directory is invalid"),

    /**
     * Root directory cannot be modified.
     */
    CANNOT_MODIFY_ROOT_DIRECTORY(2267, "Root directory cannot be modified"),

    /**
     * UUID is not sixteen bytes.
     */
    INVALID_UUID_SIZE(2268, "UUID is not sixteen bytes"),

    /**
     * Backup error.
     */
    BACKUP_ERROR(2300, "Backup error"),

    /**
     * Restore error.
     */
    RESTORE_ERROR(2301, "Restore error"),

    /**
     * Backup duplicate request.
     */
    BACKUP_DUPLICATE(2311, "Backup duplicate request"),

    /**
     * Backup unneeded request.
     */
    BACKUP_UNNEEDED(2312, "Backup unneeded request"),

    /**
     * Backup file block size too small.
     */
    BACKUP_BAD_BLOCK_SIZE(2313, "Backup file block size too small"),

    /**
     * Backup Container URL invalid.
     */
    BACKUP_INVALID_URL(2314, "Backup Container URL invalid"),

    /**
     * Backup Container URL invalid.
     */
    BACKUP_INVALID_INFO(2315, "Backup Container URL invalid"),

    /**
     * Cannot expire requested data from backup without violating minimum restorability.
     */
    BACKUP_CANNOT_EXPIRE(2316, "Cannot expire requested data from backup without violating minimum restorability"),

    /**
     * Cannot find authentication details (such as a password or secret key) for the specified Backup Container URL.
     */
    BACKUP_AUTH_MISSING(2317,
      "Cannot find authentication details (such as a password or secret key) for the specified Backup Container URL"),

    /**
     * Cannot read or parse one or more sources of authentication information for Backup Container URLs.
     */
    BACKUP_AUTH_UNREADABLE(2318,
      "Cannot read or parse one or more sources of authentication information for Backup Container URLs"),

    /**
     * Invalid restore version.
     */
    RESTORE_INVALID_VERSION(2361, "Invalid restore version"),

    /**
     * Corrupted backup data.
     */
    RESTORE_CORRUPTED_DATA(2362, "Corrupted backup data"),

    /**
     * Missing backup data.
     */
    RESTORE_MISSING_DATA(2363, "Missing backup data"),

    /**
     * Restore duplicate request.
     */
    RESTORE_DUPLICATE_TAG(2364, "Restore duplicate request"),

    /**
     * Restore tag does not exist.
     */
    RESTORE_UNKNOWN_TAG(2365, "Restore tag does not exist"),

    /**
     * Unknown backup/restore file type.
     */
    RESTORE_UNKNOWN_FILE_TYPE(2366, "Unknown backup/restore file type"),

    /**
     * Unsupported backup file version.
     */
    RESTORE_UNSUPPORTED_FILE_VERSION(2367, "Unsupported backup file version"),

    /**
     * Unexpected number of bytes read.
     */
    RESTORE_BAD_READ(2368, "Unexpected number of bytes read"),

    /**
     * Backup file has unexpected padding bytes.
     */
    RESTORE_CORRUPTED_DATA_PADDING(2369, "Backup file has unexpected padding bytes"),

    /**
     * Attempted to restore into a non-empty destination database.
     */
    RESTORE_DESTINATION_NOT_EMPTY(2370, "Attempted to restore into a non-empty destination database"),

    /**
     * Attempted to restore using a UID that had been used for an aborted restore.
     */
    RESTORE_DUPLICATE_UID(2371, "Attempted to restore using a UID that had been used for an aborted restore"),

    /**
     * Invalid task version.
     */
    TASK_INVALID_VERSION(2381, "Invalid task version"),

    /**
     * Task execution stopped due to timeout, abort, or completion by another worker.
     */
    TASK_INTERRUPTED(2382, "Task execution stopped due to timeout, abort, or completion by another worker"),

    /**
     * Expected key is missing.
     */
    KEY_NOT_FOUND(2400, "Expected key is missing"),

    /**
     * An unknown error occurred.
     */
    UNKNOWN_ERROR(4000, "An unknown error occurred"),

    /**
     * An internal error occurred.
     */
    INTERNAL_ERROR(4100, "An internal error occurred");

    private static final LongMap<ErrorCode> CODE_LOOKUP;
    static {
        final ErrorCode[] codes = ErrorCode.values();
        CODE_LOOKUP = new LongMap<>(codes.length);
        for (ErrorCode code : codes) {
            if (code.getCode() != 0)
                CODE_LOOKUP.put((long)code.getCode(), code);
        }
    }

    private final int code;
    private final String description;

    ErrorCode(int code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * Get numerical error code.
     *
     * @return error code
     */
    public int getCode() {
        return this.code;
    }

    /**
     * Get human readable description.
     *
     * @return description
     */
    public String getDescription() {
        return this.description;
    }

    /**
     * Get the {@link ErrorCode} instance with the given {@linkplain #getCode error code}, if any.
     *
     * @param code numerical error code
     * @return corresponding {@link ErrorCode}, or null if none exists
     */
    public static ErrorCode forCode(int code) {
        return code == 0 ? SUCCESS : CODE_LOOKUP.get((long)code);
    }
}
