
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.kv.fdb;

/**
 * FoundationDB error codes.
 *
 * @see <a href="https://foundationdb.com/documentation/api-error-codes.html#developer-guide-error-codes">Error Codes</a>
 */
public final class ErrorCodes {

    /**
     * Success.
     */
    public static final int SUCCESS = 0;

    /**
     * Operation failed.
     */
    public static final int OPERATION_FAILED = 1000;

    /**
     * Operation timed out.
     */
    public static final int TIMED_OUT = 1004;

    /**
     * Version no longer available.
     */
    public static final int PAST_VERSION = 1007;

    /**
     * Request for future version.
     */
    public static final int FUTURE_VERSION = 1009;

    /**
     * Transaction not committed.
     */
    public static final int NOT_COMMITTED = 1020;

    /**
     * Transaction may or may not have committed.
     */
    public static final int COMMIT_UNKNOWN_RESULT = 1021;

    /**
     * Operation aborted because the transaction was cancelled.
     */
    public static final int TRANSACTION_CANCELLED = 1025;

    /**
     * Operation aborted because the transaction timed out.
     */
    public static final int TRANSACTION_TIMED_OUT = 1031;

    /**
     * Too many watches are currently set.
     */
    public static final int TOO_MANY_WATCHES = 1032;

    /**
     * Disabling read your writes also disables watches.
     */
    public static final int WATCHES_DISABLED = 1034;

    /**
     * Asynchronous operation cancelled.
     */
    public static final int OPERATION_CANCELLED = 1101;

    /**
     * The future has been released.
     */
    public static final int FUTURE_RELEASED = 1102;

    /**
     * A platform error occurred.
     */
    public static final int PLATFORM_ERROR = 1500;

    /**
     * Large block allocation failed.
     */
    public static final int LARGE_ALLOC_FAILED = 1501;

    /**
     * QueryPerformanceCounter doesn’t work.
     */
    public static final int PERFORMANCE_COUNTER_ERROR = 1502;

    /**
     * A disk i/o operation failed.
     */
    public static final int IO_ERROR = 1510;

    /**
     * File not found.
     */
    public static final int FILE_NOT_FOUND = 1511;

    /**
     * Unable to bind to network.
     */
    public static final int BIND_FAILED = 1512;

    /**
     * File could not be read from.
     */
    public static final int FILE_NOT_READABLE = 1513;

    /**
     * File could not be written to.
     */
    public static final int FILE_NOT_WRITABLE = 1514;

    /**
     * No cluster file found in current directory or default location.
     */
    public static final int NO_CLUSTER_FILE_FOUND = 1515;

    /**
     * Cluster file too large to be read.
     */
    public static final int CLUSTER_FILE_TOO_LARGE = 1516;

    /**
     * The client made an invalid API call.
     */
    public static final int CLIENT_INVALID_OPERATION = 2000;

    /**
     * Commit with incomplete read.
     */
    public static final int COMMIT_READ_INCOMPLETE = 2002;

    /**
     * The test specification is invalid.
     */
    public static final int TEST_SPECIFICATION_INVALID = 2003;

    /**
     * The specified key was outside the legal range.
     */
    public static final int KEY_OUTSIDE_LEGAL_RANGE = 2004;

    /**
     * The specified range has a begin key larger than the end key.
     */
    public static final int INVERTED_RANGE = 2005;

    /**
     * An invalid value was passed with the specified option.
     */
    public static final int INVALID_OPTION_VALUE = 2006;

    /**
     * Option not valid in this context.
     */
    public static final int INVALID_OPTION = 2007;

    /**
     * Action not possible before the network is configured.
     */
    public static final int NETWORK_NOT_SETUP = 2008;

    /**
     * Network can be configured only once.
     */
    public static final int NETWORK_ALREADY_SETUP = 2009;

    /**
     * Transaction already has a read version set.
     */
    public static final int READ_VERSION_ALREADY_SET = 2010;

    /**
     * Version not valid.
     */
    public static final int VERSION_INVALID = 2011;

    /**
     * getRange limits not valid.
     */
    public static final int RANGE_LIMITS_INVALID = 2012;

    /**
     * Database name not supported in this version.
     */
    public static final int INVALID_DATABASE_NAME = 2013;

    /**
     * Attribute not found in string.
     */
    public static final int ATTRIBUTE_NOT_FOUND = 2014;

    /**
     * The future has not been set.
     */
    public static final int FUTURE_NOT_SET = 2015;

    /**
     * The future is not an error.
     */
    public static final int FUTURE_NOT_ERROR = 2016;

    /**
     * An operation was issued while a commit was outstanding.
     */
    public static final int USED_DURING_COMMIT = 2017;

    /**
     * An invalid atomic mutation type was issued.
     */
    public static final int INVALID_MUTATION_TYPE = 2018;

    /**
     * Incompatible protocol version.
     */
    public static final int INCOMPATIBLE_PROTOCOL_VERSION = 2100;

    /**
     * Transaction too large.
     */
    public static final int TRANSACTION_TOO_LARGE = 2101;

    /**
     * Key too large.
     */
    public static final int KEY_TOO_LARGE = 2102;

    /**
     * Value too large.
     */
    public static final int VALUE_TOO_LARGE = 2103;

    /**
     * Connection string invalid.
     */
    public static final int CONNECTION_STRING_INVALID = 2104;

    /**
     * Local address in use.
     */
    public static final int ADDRESS_IN_USE = 2105;

    /**
     * Invalid local address.
     */
    public static final int INVALID_LOCAL_ADDRESS = 2106;

    /**
     * TLS error.
     */
    public static final int TLS_ERROR = 2107;

    /**
     * API version must be set.
     */
    public static final int API_VERSION_UNSET = 2200;

    /**
     * API version may be set only once.
     */
    public static final int API_VERSION_ALREADY_SET = 2201;

    /**
     * API version not valid.
     */
    public static final int API_VERSION_INVALID = 2202;

    /**
     * API version not supported in this version or binding.
     */
    public static final int API_VERSION_NOT_SUPPORTED = 2203;

    /**
     * EXACT streaming mode requires limits, but none were given.
     */
    public static final int EXACT_MODE_WITHOUT_LIMITS = 2210;

    /**
     * An unknown error occurred.
     */
    public static final int UNKNOWN_ERROR = 4000;

    /**
     * An internal error occurred.
     */
    public static final int INTERNAL_ERROR = 4100;

    private ErrorCodes() {
    }
}

