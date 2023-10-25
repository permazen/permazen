
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Converter;

import io.permazen.core.EncodingIds;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

import java.io.Serializable;

/**
 * {@link InternetAddress} email address field type.
 *
 * <p>
 * Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> the method {@link InternetAddress#equals InternetAddress.equals()} performs a case-insensitive
 * comparison of email addresses, ignoring the personal name, whereas this field type distinguishes instances
 * that are not exactly equal.
 */
public class InternetAddressType extends StringEncodedType<InternetAddress> {

    private static final long serialVersionUID = 289940859247032224L;

    public InternetAddressType() {
        super(EncodingIds.builtin("InternetAddress"), InternetAddress.class, new InternetAddressConverter());
    }

// EmailConverter

    private static class InternetAddressConverter extends Converter<InternetAddress, String> implements Serializable {

        private static final long serialVersionUID = 3837763387234872160L;

        @Override
        protected String doForward(InternetAddress address) {
            if (address == null)
                return null;
            return address.toString();
        }

        @Override
        protected InternetAddress doBackward(String string) {
            if (string == null)
                return null;
            try {
                return new InternetAddress(string);
            } catch (AddressException e) {
                throw new IllegalArgumentException("invalid email address \"" + string + "\"", e);
            }
        }
    }
}
