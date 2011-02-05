
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.net;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods for working with IPv4 addresses and netmasks.
 */
public final class IPv4Util {

    public static final int MIN_VALID_NETMASK_WIDTH = 8;
    public static final int MAX_VALID_NETMASK_WIDTH = 30;

    private IPv4Util() {
    }

    /**
     * Convert from 32-bit integer representation.
     */
    public static Inet4Address toAddress(int address) {
        return IPv4Util.toAddress(new byte[] {
            (byte)((address >> 24) & 0xff),
            (byte)((address >> 16) & 0xff),
            (byte)((address >> 8) & 0xff),
            (byte)(address & 0xff),
        });
    }

    /**
     * Convert to 32-bit integer representation.
     */
    public static int toInt32(Inet4Address address) {
        byte[] bytes = address.getAddress();
        return ((bytes[0] & 0xff) << 24)
             | ((bytes[1] & 0xff) << 16)
             | ((bytes[2] & 0xff) << 8)
             | (bytes[3] & 0xff);
    }

    /**
     * Convert to an unsigned 32-bit integer representation within a 64-bit long.
     */
    public static long toUInt32(Inet4Address address) {
        return toInt32(address) & 0x00000000ffffffffL;
    }

    /**
     * Convert raw bytes to an {@link Inet4Address}.
     *
     * @throws IllegalArgumentException if array does not have length four
     */
    public static Inet4Address toAddress(byte[] bytes) {
        if (bytes.length != 4)
            throw new IllegalArgumentException("array has length " + bytes.length + " != 4");
        try {
            return (Inet4Address)InetAddress.getByAddress(toString(bytes), bytes);
        } catch (UnknownHostException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    /**
     * Convert an IPv4 address to string representation.
     *
     * @throws IllegalArgumentException if array does not have length four
     */
    public static String toString(Inet4Address address) {
        return IPv4Util.toString(address.getAddress());
    }

    /**
     * Convert a network address (IPv4 base address and netmask) to string representation.
     *
     * @throws IllegalArgumentException if array does not have length four
     */
    public static String toString(Inet4Address address, Inet4Address netmask) {
        return IPv4Util.toString(address) + "/" + IPv4Util.getWidth(netmask);
    }

    private static String toString(byte[] bytes) {
        assert bytes.length == 4;
        return (bytes[0] & 0xff) + "." + (bytes[1] & 0xff) + "." + (bytes[2] & 0xff) + "." + (bytes[3] & 0xff);
    }

    /**
     * Convert from string representation.
     *
     * @throws IllegalArgumentException if string is not an IP address
     */
    public static Inet4Address fromString(String string) {
        String bytepat = "([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])";
        Pattern pattern = Pattern.compile(bytepat + "\\." + bytepat + "\\." + bytepat + "\\." + bytepat);
        Matcher matcher = pattern.matcher(string);
        if (!matcher.matches())
            throw new IllegalArgumentException("invalid IP address: " + string);
        return toAddress(new byte[] {
          (byte)Integer.parseInt(matcher.group(1)),
          (byte)Integer.parseInt(matcher.group(2)),
          (byte)Integer.parseInt(matcher.group(3)),
          (byte)Integer.parseInt(matcher.group(4))
        });
    }

    /**
     * Get netmask width.
     *
     * @return a value between zero and 32 (inclusive)
     * @throws IllegalArgumentException if the value is not a valid CIDR netmask
     */
    public static int getWidth(Inet4Address netmask) {
        int mask = toInt32(netmask);
        if (mask == 0)
            return 0;
        for (int shift = 0; shift < 32; shift++) {
            if (mask == (~0 << shift))
                return 32 - shift;
        }
        throw new IllegalArgumentException("invalid netmask " + netmask);
    }

    /**
     * Determine if the given address is a valid CIDR netmask.
     * The netmask must have a width between 8 and 30 (inclusive).
     */
    public static boolean isValidNetmask(Inet4Address netmask) {
        try {
            int width = getWidth(netmask);
            return width >= MIN_VALID_NETMASK_WIDTH && width <= MAX_VALID_NETMASK_WIDTH;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /**
     * Get netmask with the given width.
     *
     * @throws IllegalArgumentException if width is less than zero or greater than 32
     */
    public static Inet4Address getNetmaskForWidth(int width) {
        if (width < 0 || width > 32)
            throw new IllegalArgumentException("invalid netmask width " + width);
        return toAddress(~0 << (32 - width));
    }

    /**
     * Get the base address (all host bits zero) of the given IP network.
     *
     * @param address any IP address on the network
     * @param netmask the netmask of the network
     * @throws IllegalArgumentException if {@code netmask} is not a valid netmask
     */
    public static Inet4Address getBaseAddress(Inet4Address address, Inet4Address netmask) {
        int addr = toInt32(address);
        int width = getWidth(netmask);
        addr &= ~0 << (32 - width);
        return toAddress(addr);
    }

    /**
     * Get the broadcast address (all host bits one) of the given IP network.
     *
     * @param address any IP address on the network
     * @param netmask the netmask of the network
     * @throws IllegalArgumentException if {@code netmask} is not a valid netmask
     */
    public static Inet4Address getBroadcastAddress(Inet4Address address, Inet4Address netmask) {
        int addr = toInt32(address);
        int width = getWidth(netmask);
        addr |= ~(~0 << (32 - width));
        return toAddress(addr);
    }

    /**
     * Determine if the given address lives on the given network.
     *
     * @param address IP address in question
     * @param network network address
     * @param netmask network netmask
     * @throws IllegalArgumentException if {@code netmask} is not a valid netmask
     */
    public static boolean isOnNetwork(Inet4Address address, Inet4Address network, Inet4Address netmask) {
        int addr1 = toInt32(address);
        int addr2 = toInt32(network);
        int width = getWidth(netmask);
        int mask = ~0 << (32 - width);
        return (addr1 & mask) == (addr2 & mask);
    }

    /**
     * Determine if the given address is a valid host address on the given network.
     * The address must lie on the given network and not equal either the base or broadcast addresses.
     *
     * @param address IP address in question
     * @param network network address
     * @param netmask network netmask
     * @throws IllegalArgumentException if {@code netmask} is not a valid netmask
     */
    public static boolean isValidHostOnNetwork(Inet4Address address, Inet4Address network, Inet4Address netmask) {
        return isOnNetwork(address, network, netmask)
         && !address.equals(getBaseAddress(network, netmask))
         && !address.equals(getBroadcastAddress(network, netmask));
    }
}

