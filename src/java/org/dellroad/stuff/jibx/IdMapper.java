
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.util.Map;

import org.dellroad.stuff.java.IdGenerator;
import org.jibx.extras.IdDefRefMapperBase;
import org.jibx.runtime.IMarshallable;
import org.jibx.runtime.IMarshallingContext;
import org.jibx.runtime.JiBXException;
import org.jibx.runtime.impl.MarshallingContext;

/**
 * JiBX Marshaller/Unmarshaller that assigns unique ID's to each object and
 * replaces duplicate appearances of the same object with an IDREF reference.
 *
 * <p>
 * This class allows for easy ID/IDREF handling for existing classes, with minimal
 * modifications to those classes and no custom (un)marshaller subclasses.
 *
 * <h3>JiBX Mapping</h3>
 *
 * <p>
 * Suppose you have a class {@code Person.java} with a single {@code name} property
 * and you want to add ID/IDREF support to it.
 *
 * <p>
 * First add the following two pseudo-bean property methods to the classes:
 *
 * <blockquote><pre>
 *  import org.dellroad.stuff.jibx.IdMapper;
 *
 *  public class Person {
 *
 *      private String name;
 *
 *      public String getName() {
 *          return this.name;
 *      }
 *      public void setName(String name) {
 *          this.name = name;
 *      }
 *
 *      <b>// JiBX methods
 *      private String getJiBXID() {
 *          return IdMapper.getId(this);
 *      }
 *      private void setJiBXID(String id) {
 *          // do nothing
 *      }</b>
 *  }
 * </pre></blockquote>
 * Note: if you subclass {@code Person.java} from a different sub-package, you may need
 * to change the access privileges of those methods from {@code private} to {@code protected}.
 * </p>
 *
 * <p>
 * Next, define a concrete mapping for {@code Person.java} and add the {@code id} attribute:
 * <blockquote><pre>
 *   &lt;mapping name="Person" class="com.example.Person"&gt;
 *       <b>&lt;value name="id" style="attribute" ident="def"
 *         get-method="getJiBXID" set-method="setJiBXID"/&gt;</b>
 *       &lt;value name="name" field="name"/&gt;
 *   &lt;/mapping&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Finally, use {@link IdMapper} as the custom marshaller and unmarshaller wherever a {@code Person} appears, e.g.:
 * <blockquote><pre>
 *   &lt;mapping name="Company" class="com.example.Company"&gt;
 *       &lt;collection name="Employees" field="employees" create-type="java.util.ArrayList"&gt;
 *          &lt;structure name="Person" type="com.example.Person"
 *            <b>marshaller="org.dellroad.stuff.jibx.IdMapper"
 *            unmarshaller="org.dellroad.stuff.jibx.IdMapper"</b>/&gt;
 *       &lt;/collection&gt;
 *       &lt;structure name="EmployeeOfTheWeek"&gt;
 *          &lt;structure name="Person" field="employeeOfTheWeek"
 *            <b>marshaller="org.dellroad.stuff.jibx.IdMapper"
 *            unmarshaller="org.dellroad.stuff.jibx.IdMapper"</b>/&gt;
 *       &lt;/structure&gt;
 *   &lt;/mapping&gt;
 * </pre></blockquote>
 * Note the {@code EmployeeOfTheWeek} "wrapper" element for the {@code employeeOfTheWeek} field; this is required
 * in order to use an XML name for this field other than {@code Person} (see limitations below).
 * </p>
 *
 * <p>
 * Now the first appearance of any {@code Person} will contain the full XML structure with an additional <code>id="..."</code>
 * attribute, while all subsequent appearances will contain just a reference of the form <code>&lt;Person idref="..."/&gt;</code>.
 * Conversely, when unmarshalled all {@code Person} XML elements that refer to the same original {@code Person} will
 * re-use the same unmarshalled {@code Person} object.
 * </p>
 *
 * <p>
 * So the resulting XML might look like:
 * <blockquote><pre>
 *  &lt;Company&gt;
 *      &lt;Employees&gt;
 *          &lt;Person id="N00001"&gt;
 *              &lt;name&gt;Aardvark, Annie&lt;/name&gt;
 *          &lt;/Person&gt;
 *          &lt;Person id="N00002"&gt;
 *              &lt;name&gt;Appleby, Arnold&lt;/name&gt;
 *          &lt;/Person&gt;
 *          ...
 *      &lt;/Employees&gt;
 *      &lt;EmployeeOfTheWeek&gt;
 *          &lt;Person idref="N00001"/&gt;
 *      &lt;/EmployeeOfTheWeek&gt;
 *  &lt;/Company&gt;
 * </pre></blockquote>
 * </p>
 *
 * <h3>Limitations</h3>
 *
 * <p>
 * JiBX and this class impose some limitations:
 * <ul>
 * <li>JiBX marshalling must be peformed within an invocation of {@link IdGenerator#run IdGenerator.run()}
 *      so that an {@link IdGenerator} is available to generate the unique IDs (for help
 *      when using Spring, see {@link IdMappingMarshaller}).</li>
 * <li>Classes that use ID/IDREF must have concrete JiBX mappings.</li>
 * <li>All occurences of the class must use the XML element name of the concrete mapping, so the use of
 *      a "wrapper" element is required when a different element name is desired.</li>
 * </ul>
 *
 * @see IdMappingMarshaller
 */
public class IdMapper extends IdDefRefMapperBase {

    private final String uri;
    private final int index;
    private final String name;
    private final String className;

    // This is here to work around bogus JiBX binding error
    private IdMapper() {
        super(null, 0, null);
        throw new UnsupportedOperationException();
    }

    public IdMapper(String uri, int index, String name, String className) {
        super(uri, index, name);
        this.uri = uri;
        this.index = index;
        this.name = name;
        this.className = className;
    }

    /**
     * Get the id value for the given object.
     *
     * <p>
     * The implementation in {@link IdMapper} formats an ID of the form <code>N012345</code>
     * using the {@link IdGenerator} acquired from {@link IdGenerator#get}.
     */
    public static String getId(Object obj) {
        return String.format("N%05d", IdGenerator.get().getId(obj));
    }

    /**
     * Get the unique ID for the given object. Delegates to {@link #getId getId()}.
     */
    @Override
    protected String getIdValue(Object obj) {
        return IdMapper.getId(obj);
    }

    /**
     * Get the ID reference attribute name. Default is <code>"idref"</code>.
     */
    @Override
    protected String getAttributeName() {
        return "idref";
    }

    /**
     * Overrides superclass to use object equality instead of {@code Object.equals()} for sanity checking.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void marshal(Object obj, IMarshallingContext ictx) throws JiBXException {

        // Sanity check
        if (obj == null)
            return;
        if (!(ictx instanceof MarshallingContext))
            throw new JiBXException("Invalid context type for marshaller");

        // Check if ID already defined
        MarshallingContext ctx = (MarshallingContext)ictx;
        Map<String, Object> map = (Map<String, Object>)ctx.getIdMap();
        String id = this.getIdValue(obj);
        Object value = map.get(id);

        // New object? Output normally
        if (value == null) {
            if (!(obj instanceof IMarshallable))
                throw new JiBXException("instance of " + obj.getClass() + " is not marshallable");
            map.put(id, obj);
            ((IMarshallable)obj).marshal(ctx);
            return;
        }

        // Sanity check what we got
        if (value != obj)
            throw new JiBXException("encountered two objects with the same ID " + id);

        // Emit a reference
        ctx.startTagAttributes(this.index, this.name);
        ctx.attribute(0, this.getAttributeName(), id);
        ctx.closeStartEmpty();
    }
}

