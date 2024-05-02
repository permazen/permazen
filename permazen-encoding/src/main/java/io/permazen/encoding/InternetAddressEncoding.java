
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Converter;

import jakarta.mail.internet.AddressException;
import jakarta.mail.internet.InternetAddress;

import java.io.Serializable;

/**
 * {@link InternetAddress} email address encoding.
 *
 * <p>
 * Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> the method {@link InternetAddress#equals InternetAddress.equals()} performs a case-insensitive
 * comparison of email addresses, ignoring the personal name, whereas this encoding distinguishes instances
 * that are not exactly equal.
 */
public class InternetAddressEncoding extends StringConvertedEncoding<InternetAddress> {

    private static final long serialVersionUID = 289940859247032224L;

    public InternetAddressEncoding(EncodingId encodingId) {
        super(encodingId, InternetAddress.class, new InternetAddressConverter());
    }

// InternetAddressConverter

    // This is a separate class instead of using Converter.from() to avoid early linkage to optional InternetAddress class
    private static class InternetAddressConverter extends Converter<InternetAddress, String> implements Serializable {

        private static final long serialVersionUID = 3837763387234872160L;

        @Override
        protected String doForward(InternetAddress address) {
            assert address != null;
            return address.toString();
        }

        @Override
        protected InternetAddress doBackward(String string) {
            assert string != null;
            try {
                return new InternetAddress(string);
            } catch (AddressException e) {
                throw new IllegalArgumentException("invalid email address \"" + string + "\"", e);
            }
        }
    }
}
