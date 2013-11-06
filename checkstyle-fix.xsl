<?xml version="1.0" encoding="ISO-8859-1"?>

<!-- $Id$ -->
<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <xsl:output doctype-public="-//Puppy Crawl//DTD Check Configuration 1.1//EN"
      doctype-system="http://www.puppycrawl.com/dtds/configuration_1_1.dtd"/>

    <xsl:template match="module[@name = 'RedundantThrows']">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
            <property name="suppressLoadErrors" value="true"/>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="@*|node()|processing-instruction()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()|processing-instruction()"/>
        </xsl:copy>
    </xsl:template>

</xsl:transform>
