<?xml version="1.0" encoding="ISO-8859-1"?>

<!-- $Id$ -->
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:output encoding="ISO-8859-1" method="text"/>

    <xsl:template match="/">
        <xsl:value-of select="'#!/bin/sh&#10;'"/>
        <xsl:value-of select="'# GENERATED FILE - DO NOT EDIT&#10;'"/>
        <xsl:value-of select="'# Style: $Id$&#10;'"/>
        <xsl:value-of select="'&#10;'"/>
        <xsl:apply-templates/>
    </xsl:template>

    <xsl:template match="file[error[
      (@source='com.puppycrawl.tools.checkstyle.checks.imports.UnusedImportsCheck'
        and starts-with(@message, 'Unused import -'))
    or
      (@source='com.puppycrawl.tools.checkstyle.checks.imports.RedundantImportCheck'
        and starts-with(@message, 'Redundant import from the same package -'))
    or
      (@source='com.puppycrawl.tools.checkstyle.checks.regexp.RegexpSinglelineCheck'
        and @message = 'Line has trailing spaces.')
    ]]">
        <xsl:variable name="file" select="concat(&quot;'&quot;, @name, &quot;'&quot;)"/>
        <xsl:variable name="temp" select="concat(&quot;'&quot;, @name, '.unused-imports-new', &quot;'&quot;)"/>
        <xsl:value-of select="'sed \&#10;'"/>

        <!-- Delete redundant import lines -->
        <xsl:for-each select="error[
          (@source='com.puppycrawl.tools.checkstyle.checks.imports.UnusedImportsCheck'
            and starts-with(@message, 'Unused import -'))
        or
          (@source='com.puppycrawl.tools.checkstyle.checks.imports.RedundantImportCheck'
            and starts-with(@message, 'Redundant import from the same package -'))
        ]">
            <xsl:value-of select="concat('  -e ', @line, 'd \&#10;')"/>
        </xsl:for-each>

        <!-- Remove trailing whitespace -->
        <xsl:for-each select="error[
          (@source='com.puppycrawl.tools.checkstyle.checks.regexp.RegexpSinglelineCheck'
            and @message = 'Line has trailing spaces.')
        ]">
            <xsl:value-of select="concat('  -e ', &quot;'&quot;, 's/[[:space:]]\{1,\}$//g', &quot;'&quot;, ' \&#10;')"/>
        </xsl:for-each>

        <xsl:value-of select="concat('  &lt; ', $file, ' \&#10;  &gt; ', $temp,
          ' \&#10;  &amp;&amp; mv ', $temp, ' \&#10;    ', $file, '&#10;')"/>
    </xsl:template>

    <xsl:template match="@*|node()">
        <xsl:apply-templates/>
    </xsl:template>

</xsl:transform>
