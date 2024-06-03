
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;

import java.util.Arrays;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

/**
 * XML related utility methods.
 */
public final class XMLUtil {

    private XMLUtil() {
    }

    /**
     * Determine if the given character may appear in an XML document.
     *
     * @see <a href="https://www.w3.org/TR/REC-xml/#charsets">The XML 1.0 Specification</a>
     *
     * @param codepoint character codepoint
     * @return true if {@code codepoint} is a valid XML character
     */
    public static boolean isValidChar(int codepoint) {
        return (codepoint >= 0x0020 && codepoint <= 0xd7ff)
          || (codepoint == '\n' || codepoint == '\r' || codepoint == '\t')
          || (codepoint >= 0xe000 && codepoint <= 0xfffd)
          || (codepoint >= 0x10000 && codepoint <= 0x10ffff);
    }

    /**
     * Determine if the given string is a valid XML name.
     *
     * @see <a href="https://www.w3.org/TR/REC-xml/#sec-common-syn">The XML 1.0 Specification</a>
     *
     * @param name name
     * @return true if {@code name} is a valid XML name
     * @throws IllegalArgumentException if {@code name} is null
     */
    public static boolean isValidName(String name) {
        Preconditions.checkArgument(name != null, "null name");
        Preconditions.checkArgument(!name.isEmpty(), "empty name");
        int codepoint;
        for (int i = 0; i < name.length(); i += Character.charCount(codepoint)) {
            codepoint = name.codePointAt(i);
            final boolean valid = i == 0 ? XMLUtil.isValidNameStart(codepoint) : XMLUtil.isValidNamePart(codepoint);
            if (!valid)
                return false;
        }
        return true;
    }

    /**
     * Determine if a character may appear in an XML name as the first character.
     *
     * <p>
     * If so, the character may appear anywhere in an XML element or attribute name.
     *
     * @see <a href="https://www.w3.org/TR/REC-xml/#sec-common-syn">The XML 1.0 Specification</a>
     *
     * @param codepoint character
     * @return true if {@code ch} is valid at the beginning of an XML name
     */
    public static boolean isValidNameStart(int codepoint) {
         return
              (codepoint >= 'A' && codepoint <= 'Z')
           || (codepoint >= 'a' && codepoint <= 'z')
           || codepoint == ':'
           || codepoint == '_'
           || (codepoint >= 0x00c0 && codepoint <= 0x00d6)
           || (codepoint >= 0x00d8 && codepoint <= 0x00f6)
           || (codepoint >= 0x00f8 && codepoint <= 0x02ff)
           || (codepoint >= 0x0370 && codepoint <= 0x037d)
           || (codepoint >= 0x0d7f && codepoint <= 0x1fff)
           || (codepoint >= 0x200c && codepoint <= 0x200d)
           || (codepoint >= 0x2070 && codepoint <= 0x218f)
           || (codepoint >= 0x2c00 && codepoint <= 0x2fef)
           || (codepoint >= 0x3001 && codepoint <= 0xd7ff)
           || (codepoint >= 0xf900 && codepoint <= 0xfdcf)
           || (codepoint >= 0xfdf0 && codepoint <= 0xfffd)
           || (codepoint >= 0x10000 && codepoint <= 0xeffff);
    }

    /**
     * Determine if a character may appear in an XML name.
     *
     * <p>
     * If so, the character may appear in an XML element or attribute name, though not necessarily as the first character.
     *
     * @see <a href="https://www.w3.org/TR/REC-xml/#sec-common-syn">The XML 1.0 Specification</a>
     *
     * @param codepoint character
     * @return true if {@code ch} is valid in an XML name
     */
    public static boolean isValidNamePart(int codepoint) {
        if (XMLUtil.isValidNameStart(codepoint))
            return true;
        switch (codepoint) {
        case '-':
        case '.':
        case '0':
        case '1':
        case '2':
        case '3':
        case '4':
        case '5':
        case '6':
        case '7':
        case '8':
        case '9':
        case 0x00b7:
            return true;
        default:
             return
                  (codepoint >= 0x0300 && codepoint <= 0x036f)
               || (codepoint >= 0x203f && codepoint <= 0x2040);
        }
    }
}
