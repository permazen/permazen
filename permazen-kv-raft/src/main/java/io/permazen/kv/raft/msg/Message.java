
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.raft.msg;

import com.google.common.base.Preconditions;

import io.permazen.kv.raft.Timestamp;
import io.permazen.util.ByteData;
import io.permazen.util.ByteUtil;
import io.permazen.util.LongEncoder;
import io.permazen.util.UnsignedIntEncoder;

import java.io.StringWriter;
import java.nio.ByteBuffer;

/**
 * Support superclass for Raft messages.
 */
public abstract class Message {

    // Codes identifying serialized message type
    static final byte APPEND_REQUEST_TYPE = 1;
    static final byte APPEND_RESPONSE_TYPE = 2;
    static final byte COMMIT_REQUEST_TYPE = 3;
    static final byte COMMIT_RESPONSE_TYPE = 4;
    static final byte GRANT_VOTE_TYPE = 5;
    static final byte INSTALL_SNAPSHOT_TYPE = 6;
    static final byte REQUEST_VOTE_TYPE = 7;
    static final byte PING_REQUEST_TYPE = 8;
    static final byte PING_RESPONSE_TYPE = 9;
    static final byte MAX_TYPE = 10;

    // Serialization version number
    static final int VERSION_1 = 1;
    static final int VERSION_2 = 2;
    static final int VERSION_3 = 3;

    // Minimum buffer size to use a direct buffer
    private static final int MIN_DIRECT_BUFFER_SIZE = 128;

    private final byte type;
    private final int clusterId;
    private final String senderId;
    private final String recipientId;
    private final long term;

    protected Message(byte type, int clusterId, String senderId, String recipientId, long term) {
        this.type = type;
        this.clusterId = clusterId;
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.term = term;
    }

    protected Message(byte type, ByteBuffer buf, int version) {
        this.type = type;
        this.clusterId = buf.getInt();
        this.senderId = Message.getString(buf);
        this.recipientId = Message.getString(buf);
        this.term = LongEncoder.read(buf);
    }

    void checkArguments() {
        Preconditions.checkArgument(this.type > 0 && this.type < MAX_TYPE);
        Preconditions.checkArgument(this.clusterId != 0);
        Preconditions.checkArgument(this.senderId != null);
        Preconditions.checkArgument(this.recipientId != null);
        Preconditions.checkArgument(this.term > 0);
    }

// Properties

    /**
     * Get the cluster ID of the sender.
     *
     * @return sender's cluster ID
     */
    public int getClusterId() {
        return this.clusterId;
    }

    /**
     * Get the identity of the sender.
     *
     * @return sender's unique identity string
     */
    public String getSenderId() {
        return this.senderId;
    }

    /**
     * Get the identity of the recipient.
     *
     * @return recipient's unique identity string
     */
    public String getRecipientId() {
        return this.recipientId;
    }

    /**
     * Get the term of the sender of this message.
     *
     * @return requester's unique ID
     */
    public long getTerm() {
        return this.term;
    }

    /**
     * Determine whether this message is only sent by leaders.
     *
     * @return true if receipt of this message implies sender is a leader
     */
    public boolean isLeaderMessage() {
        return false;
    }

// MessageSwitch

    /**
     * Apply the visitor pattern based on this instance's type.
     *
     * @param handler target for visit
     */
    public abstract void visit(MessageSwitch handler);

// (De)serialization methods

    // Check encoding format version
    static void checkVersion(int version) {
        switch (version) {
        case Message.VERSION_1:
        case Message.VERSION_2:
        case Message.VERSION_3:
            break;
        default:
            throw new IllegalArgumentException(String.format("unrecognized message format version %s", version));
        }
    }

    /**
     * Get the current (i.e., maximum known) protocol version number.
     *
     * @return latest message protocol version
     */
    public static int getCurrentProtocolVersion() {
        return Message.VERSION_3;
    }

    /**
     * Read the protocol version number header from the message encoded in the given input.
     *
     * @param buf source for encoded message
     * @return decoded message protocol version
     */
    public static int decodeProtocolVersion(ByteBuffer buf) {
        return buf.get() & 0xff;
    }

    /**
     * Decode a message from the given input. The protocol version number header should already have been read.
     *
     * <p>
     * Note that data is not necessarily copied out of {@code buf}, so the returned instance may become invalid
     * if the data in {@code buf} gets overwritten.
     *
     * @param buf source for encoded message
     * @param version message version
     * @return decoded message
     * @throws java.nio.BufferUnderflowException if there is not enough data
     * @throws IllegalArgumentException if {@code version} is bogus
     * @throws IllegalArgumentException if encoded message is bogus
     * @throws IllegalArgumentException if there is trailing garbage
     */
    public static Message decode(ByteBuffer buf, int version) {

        // Check encoding format version
        Message.checkVersion(version);

        // Read type and decode message
        final Message msg;
        final byte type = buf.get();
        switch (type) {
        case APPEND_REQUEST_TYPE:
            msg = new AppendRequest(buf, version);
            break;
        case APPEND_RESPONSE_TYPE:
            msg = new AppendResponse(buf, version);
            break;
        case COMMIT_REQUEST_TYPE:
            msg = new CommitRequest(buf, version);
            break;
        case COMMIT_RESPONSE_TYPE:
            msg = new CommitResponse(buf, version);
            break;
        case GRANT_VOTE_TYPE:
            msg = new GrantVote(buf, version);
            break;
        case INSTALL_SNAPSHOT_TYPE:
            msg = new InstallSnapshot(buf, version);
            break;
        case REQUEST_VOTE_TYPE:
            msg = new RequestVote(buf, version);
            break;
        case PING_REQUEST_TYPE:
            msg = new PingRequest(buf, version);
            break;
        case PING_RESPONSE_TYPE:
            msg = new PingResponse(buf, version);
            break;
        default:
            throw new IllegalArgumentException(String.format("invalid message type %s", type));
        }

        // Check for trailing garbage
        if (buf.hasRemaining()) {
            throw new IllegalArgumentException(String.format(
              "buffer contains %d bytes of extra garbage after %s", buf.remaining(), msg));
        }

        // Done
        return msg;
    }

    /**
     * Serialize this instance.
     *
     * @param version protocol encoding version number
     * @throws IllegalArgumentException if {@code version} is bogus
     * @return encoded message
     */
    public ByteBuffer encode(int version) {

        // Check encoding format version
        Message.checkVersion(version);

        // Allocate buffer
        final int size = this.calculateSize(version);
        final ByteBuffer buf = size >= MIN_DIRECT_BUFFER_SIZE ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);

        // Encode message
        this.writeTo(buf, version);
        if (buf.hasRemaining()) {
            throw new RuntimeException(String.format(
              "internal error: %d remaining bytes in buffer from %s", buf.remaining(), this));
        }
        return buf.flip();
    }

    /**
     * Serialize this instance into the given buffer.
     *
     * @param buf destination for encoded data
     * @param version protocol encoding version number
     * @throws java.nio.BufferOverflowException if data overflows {@code buf}
     * @throws IllegalArgumentException if {@code version} is bogus
     */
    public void writeTo(ByteBuffer buf, int version) {
        Message.checkVersion(version);
        buf.put((byte)version);
        buf.put(this.type);
        buf.putInt(this.clusterId);
        Message.putString(buf, this.senderId);
        Message.putString(buf, this.recipientId);
        LongEncoder.write(buf, this.term);
    }

    /**
     * Calculate the number of bytes required by {@link #writeTo writeTo()}.
     *
     * @param version protocol encoding version
     * @return an upper bound on the number of encoded bytes
     */
    protected int calculateSize(int version) {
        Message.checkVersion(version);
        return 1                                                                    // protocol encoding version
          + 1                                                                       // message type
          + 4                                                                       // cluster ID
          + Message.calculateSize(this.senderId)
          + Message.calculateSize(this.recipientId)
          + LongEncoder.encodeLength(this.term);
    }

// Object

    @Override
    public abstract String toString();

// Helpers

    /**
     * Serialize a {@link ByteBuffer} into the buffer.
     *
     * @param dest destination for encoded data
     * @param buf data to encode
     * @throws java.nio.ReadOnlyBufferException if {@code dest} is read only
     * @throws java.nio.BufferOverflowException if {@code dest} overflows
     * @throws IllegalArgumentException if {@code buf} has more than 2<sup>^31</sup> bytes remaining
     * @throws IllegalArgumentException if either parameter is null
     */
    protected static void putByteBuffer(ByteBuffer dest, ByteBuffer buf) {
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(buf != null, "null buf");
        UnsignedIntEncoder.write(dest, buf.remaining());
        dest.put(buf.asReadOnlyBuffer());
    }

    /**
     * Deserialize a {@link ByteBuffer} previously serialized by {@link #putByteBuffer putByteBuffer()} from the buffer.
     *
     * @param buf source for encoded data
     * @return decoded data
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if input is bogus
     * @throws IllegalArgumentException if {@code buf} is null
     */
    protected static ByteBuffer getByteBuffer(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        final int numBytes = UnsignedIntEncoder.read(buf);
        if (numBytes > buf.remaining())
            throw new IllegalArgumentException(String.format("bogus buffer length %d > %d", numBytes, buf.remaining()));
        final ByteBuffer result = buf.slice().limit(numBytes);
        buf.position(buf.position() + numBytes);
        return result;
    }

    protected static int calculateSize(ByteBuffer buf) {
        return UnsignedIntEncoder.encodeLength(buf.remaining()) + buf.remaining();
    }

    /**
     * Serialize a {@link String} into the buffer.
     *
     * @param dest destination for encoded data
     * @param string string to encode
     * @throws java.nio.ReadOnlyBufferException if {@code dest} is read only
     * @throws java.nio.BufferOverflowException if {@code dest} overflows
     * @throws IllegalArgumentException if either parameter is null
     */
    protected static void putString(ByteBuffer dest, String string) {
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(string != null, "null string");
        for (int i = 0; i < string.length(); i++) {
            final int c = string.charAt(i);
            if (c >= 0x0001 && c <= 0x007f)
                dest.put((byte)c);
            else if (c < 0x0800) {
                dest.put((byte)(0xc0 | ((c >> 6) & 0x1f)));
                dest.put((byte)(0x80 | ((c >> 0) & 0x3f)));
            } else {
                dest.put((byte)(0xe0 | ((c >> 12) & 0x0f)));
                dest.put((byte)(0x80 | ((c >>  6) & 0x3f)));
                dest.put((byte)(0x80 | ((c >>  0) & 0x3f)));
            }
        }
        dest.put((byte)0);
    }

    /**
     * Deserialize a {@link String} previously serialized by {@link #putString putString()} from the buffer.
     *
     * @param buf source for encoded data
     * @return decoded string, never null
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if input is bogus
     */
    protected static String getString(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        final StringWriter writer = new StringWriter();
        while (true) {
            final int b1 = buf.get() & 0xff;
            if (b1 == 0)
                break;
            if ((b1 & 0x80) == 0)
                writer.append((char)b1);
            else if ((b1 & 0xe0) == 0xc0) {
                final int b2 = buf.get() & 0xff;
                if ((b2 & 0xc0) != 0x80)
                    throw new IllegalArgumentException(String.format("invalid UTF-8 sequence: 0x%02x 0x%02x", b1, b2));
                writer.append((char)((b1 & 0x1f) << 6 | (b2 & 0x3f)));
            } else if ((b1 & 0xf0) == 0xe0) {
                final int b2 = buf.get() & 0xff;
                final int b3 = buf.get() & 0xff;
                if ((b2 & 0xc0) != 0x80 || (b3 & 0xc0) != 0x80)
                    throw new IllegalArgumentException(String.format("invalid UTF-8 sequence: 0x%02x 0x%02x 0x%02x", b1, b2, b3));
                writer.append((char)(((b1 & 0x0f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f)));
            } else
                throw new IllegalArgumentException(String.format("invalid UTF-8 sequence: 0x%02x", b1));
        }
        return writer.toString();
    }

    protected static int calculateSize(String string) {
        Preconditions.checkArgument(string != null, "null string");
        int total = 1;
        for (int i = 0; i < string.length(); i++) {
            final int ch = string.charAt(i);
            total += (ch != 0x0000 && ch < 0x0080) ? 1 : ch < 0x0800 ? 2 : 3;
        }
        return total;
    }

    /**
     * Serialize a boolean value into the buffer.
     *
     * @param dest destination for encoded data
     * @param value value to encode
     * @throws java.nio.ReadOnlyBufferException if {@code dest} is read only
     * @throws java.nio.BufferOverflowException if {@code dest} overflows
     * @throws IllegalArgumentException if {@code dest} is null
     */
    protected static void putBoolean(ByteBuffer dest, boolean value) {
        Preconditions.checkArgument(dest != null, "null dest");
        dest.put(value ? (byte)1 : (byte)0);
    }

    /**
     * Deserialize a boolean value previously serialized by {@link #putBoolean putBoolean()} from the buffer.
     *
     * @param buf source for encoded data
     * @return decoded value
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if input is bogus
     */
    protected static boolean getBoolean(ByteBuffer buf) {
        Preconditions.checkArgument(buf != null, "null buf");
        switch (buf.get()) {
        case (byte)0:
            return false;
        case (byte)1:
            return true;
        default:
            throw new IllegalArgumentException("read invalid boolean value");
        }
    }

    /**
     * Serialize a {@link Timestamp} value into the buffer.
     *
     * @param dest destination for encoded data
     * @param timestamp value to encode
     * @param version protocol encoding version
     * @throws java.nio.ReadOnlyBufferException if {@code dest} is read only
     * @throws java.nio.BufferOverflowException if {@code dest} overflows
     * @throws IllegalArgumentException if {@code dest} or {@code timestamp} is null
     */
    protected static void putTimestamp(ByteBuffer dest, Timestamp timestamp, int version) {
        Preconditions.checkArgument(dest != null, "null dest");
        Preconditions.checkArgument(timestamp != null, "null timestamp");
        Message.checkVersion(version);
        if (version > Message.VERSION_2)
            LongEncoder.write(dest, timestamp.getMillis());
        else
            UnsignedIntEncoder.write(dest, timestamp.getMillis());
    }

    /**
     * Deserialize a {@link Timestamp} value previously serialized by {@link #putTimestamp putTimestamp()} from the buffer.
     *
     * @param buf source for encoded data
     * @param version protocol encoding version
     * @return decoded value
     * @throws java.nio.BufferUnderflowException if {@code buf} underflows
     * @throws IllegalArgumentException if input is bogus
     */
    protected static Timestamp getTimestamp(ByteBuffer buf, int version) {
        Preconditions.checkArgument(buf != null, "null buf");
        Message.checkVersion(version);
        return new Timestamp(version > VERSION_2 ? (int)LongEncoder.read(buf) : UnsignedIntEncoder.read(buf));
    }

    protected static int calculateSize(Timestamp timestamp, int version) {
        Preconditions.checkArgument(timestamp != null, "null timestamp");
        Message.checkVersion(version);
        final int millis = timestamp.getMillis();
        return version > VERSION_2 ? LongEncoder.encodeLength(millis) : UnsignedIntEncoder.encodeLength(millis);
    }

// Debugging

    String describe(ByteBuffer buf) {
        if (buf == null)
            return null;
        final int size = buf.remaining();
        if (size <= 32) {
            final byte[] data = new byte[size];
            buf.asReadOnlyBuffer().get(data);
            return ByteUtil.toString(ByteData.of(data));
        }
        return size + " bytes";
    }
}
