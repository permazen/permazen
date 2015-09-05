<?xml version="1.0" encoding="ISO-8859-1"?>

<!--
    This extract inline <dellroad-stuff:sql-update> schema updates from a Spring XML file into an SQL script.
    This transform assumes that:
   
    - Updates are listed in the order you want to apply them
    - All updates are defined using <dellroad-stuff:sql-update> tags with inline SQL (no "resource" attributes)
-->

<xsl:transform
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:beans="http://www.springframework.org/schema/beans"
  xmlns:dellroad-stuff="http://dellroad-stuff.googlecode.com/schema/dellroad-stuff"
  version="1.0">

    <xsl:output encoding="UTF-8" method="text" media-type="text/x-sql"/>

    <xsl:param name="terminator" select="';'"/>

    <xsl:template match="/">
        <xsl:text>-- GENERATED FILE - DO NOT EDIT&#10;&#10;</xsl:text>
        <xsl:for-each select="beans:beans/dellroad-stuff:sql-update">
            <xsl:if test="@resource">
                <xsl:message terminate="yes">
                    ERROR: can't extract update SQL from a resource="..." schema update
                </xsl:message>
            </xsl:if>
            <xsl:variable name="sql">
                <xsl:call-template name="trim"/>
            </xsl:variable>
            <xsl:value-of select="concat('-- Update &quot;', @id, '&quot;&#10;', $sql)"/>
            <xsl:if test="string-length($sql) &lt; string-length($terminator)
              or substring($sql, string-length($sql) - string-length($terminator) + 1) != $terminator">
                <xsl:value-of select="$terminator"/>
            </xsl:if>
            <xsl:text>&#10;&#10;</xsl:text>
        </xsl:for-each>
    </xsl:template>

    <xsl:template name="trim">
        <xsl:param name="s" select="."/>
        <xsl:variable name="fc" select="substring($s, 1, 1)"/>
        <xsl:variable name="lc" select="substring($s, string-length($s))"/>
        <xsl:choose>
            <xsl:when test="normalize-space($fc) = ''">
                <xsl:call-template name="trim">
                    <xsl:with-param name="s" select="substring($s, 2)"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:when test="normalize-space($lc) = ''">
                <xsl:call-template name="trim">
                    <xsl:with-param name="s" select="substring($s, 1, string-length($s) - 1)"/>
                </xsl:call-template>
            </xsl:when>
            <xsl:otherwise>
                <xsl:value-of select="$s"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>

</xsl:transform>
