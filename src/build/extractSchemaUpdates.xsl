<?xml version="1.0" encoding="ISO-8859-1"?>

<!--
    This extract inline <dellroad-stuff:sql> schema updates from a Spring XML file into an SQL script.
-->

<!-- $Id$ -->
<xsl:transform
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:beans="http://www.springframework.org/schema/beans"
  xmlns:dellroad-stuff="http://dellroad-stuff.googlecode.com/schema/dellroad-stuff"
  version="1.0">

    <xsl:output encoding="UTF-8" method="text" media-type="text/x-sql"/>

    <xsl:param name="terminator" select="';'"/>

    <xsl:template match="/">
        <xsl:text>-- GENERATED FILE - DO NOT EDIT&#10;&#10;</xsl:text>
        <xsl:for-each select="beans:beans/beans:bean[@class = 'org.dellroad.stuff.schema.SpringDelegatingSchemaUpdate']">
            <xsl:variable name="sql" select="beans:property[@name = 'databaseAction']/dellroad-stuff:sql[not(@resource)]"/>
            <xsl:choose>
                <xsl:when test="$sql"/>
                <xsl:otherwise>
                    <xsl:message terminate="yes">
                        ERROR: can't extract schema update from non-standard schema update
                    </xsl:message>
                </xsl:otherwise>
            </xsl:choose>
            <xsl:value-of select="concat('-- Update &quot;', @id, '&quot;&#10;', $sql, $terminator, '&#10;&#10;')"/>
        </xsl:for-each>
    </xsl:template>

</xsl:transform>
