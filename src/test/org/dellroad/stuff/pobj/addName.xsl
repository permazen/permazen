<?xml version="1.0" encoding="utf-8"?>

<!-- $Id$ -->
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:template match="/RootObject">
        <xsl:copy>
            <xsl:apply-templates select="@*"/>
            <name>default</name>
            <xsl:apply-templates select="node()"/>
        </xsl:copy>
    </xsl:template>

    <!-- Default is to copy & recurse -->
    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|*"/>
        </xsl:copy>
    </xsl:template>

</xsl:transform>

