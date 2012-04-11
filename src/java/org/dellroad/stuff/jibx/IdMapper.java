
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
 *      private String getJiBXId() {
 *          return IdMapper.getId(this);
 *      }
 *      private void setJiBXId(String id) {
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
 *  &lt;mapping name="Person" class="com.example.Person"&gt;
 *      <b>&lt;value name="id" style="attribute" ident="def"
 *        get-method="getJiBXId" set-method="setJiBXId"/&gt;</b>
 *      &lt;value name="name" field="name"/&gt;
 *  &lt;/mapping&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Finally, use {@link IdMapper} as the custom marshaller and unmarshaller wherever a {@code Person} appears, e.g.:
 * <blockquote><pre>
 *  &lt;mapping name="Company" class="com.example.Company"&gt;
 *      &lt;collection name="Employees" field="employees" create-type="java.util.ArrayList"&gt;
 *          &lt;structure name="Person" type="com.example.Person"
 *            <b>marshaller="org.dellroad.stuff.jibx.IdMapper"
 *            unmarshaller="org.dellroad.stuff.jibx.IdMapper"</b>/&gt;
 *      &lt;/collection&gt;
 *      &lt;structure name="EmployeeOfTheWeek"&gt;
 *          &lt;structure name="Person" field="employeeOfTheWeek"
 *            <b>marshaller="org.dellroad.stuff.jibx.IdMapper"
 *            unmarshaller="org.dellroad.stuff.jibx.IdMapper"</b>/&gt;
 *      &lt;/structure&gt;
 *  &lt;/mapping&gt;
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
 * <li>JiBX marshalling must be performed within an invocation of {@link IdGenerator#run IdGenerator.run()}
 *      so that an {@link IdGenerator} is available to generate the unique IDs (when using Spring, consider using
 *      {@link IdMappingMarshaller}; otherwise, the {@link JiBXUtil} methods all satisfy this requirement).</li>
 * <li>Classes that use ID/IDREF must have concrete JiBX mappings.</li>
 * <li>All occurences of the class must use the XML element name of the concrete mapping, so the use of
 *      a "wrapper" element is required when a different element name is desired.</li>
 * </ul>
 *
 * <h3>A Simpler Approach</h3>
 *
 * The above approach is useful when you don't want to keep track of which instance of an object will appear first
 * in the XML encoding: the first one will always fully define the object, while subsequent ones will just reference it.
 *
 * <p>
 * If this flexibility is not needed, i.e., if you can identify where in your mapping the first occurrence of an object
 * will appear, then the following simpler approach works without the above approach's limitations (other than requiring
 * that marshalling be peformed within an invocation of {@link IdGenerator#run IdGenerator.run()}):
 *
 * <p>
 * First, replace the <code>// do nothing</code> in the example above with call to {@link IdMapper#setId IdMapper.setId()},
 * and add a custom deserializer delegating to {@link ParseUtil#deserializeReference ParseUtil.deserializeReference()} to
 * <blockquote><pre>
 *      private void setJiBXId(String id) {
 *          IdMapper.setId(this, id);
 *      }
 *
 *      public static Employee deserializeEmployeeReference(String string) throws JiBXParseException {
 *          return ParseUtil.deserializeReference(string, Employee.class);
 *      }
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * Then, map the first occurrence of an object exactly as in the concrete mapping above, exposing the <code>JiBXId</code> property.
 * In all subsequent occurrences of the object, expose the reference to the object as a simple property using the custom
 * serializer/deserializer pair {@link ParseUtil#serializeReference ParseUtil.serializeReference()} and
 * {@code Employee.deserializeEmployeeReference()}.
 * </p>
 *
 * <p>
 * For example, the following binding would yeild the same XML encoding as before:
 * <blockquote><pre>
 *  &lt;mapping abstract="true" type-name="person" class="com.example.Person"&gt;
 *      <b>&lt;value name="id" style="attribute" get-method="getJiBXId" set-method="setJiBXId"/&gt;</b>
 *      &lt;value name="name" field="name"/&gt;
 *  &lt;/mapping&gt;
 *
 *  &lt;mapping name="Company" class="com.example.Company"&gt;
 *      &lt;collection name="Employees" field="employees" create-type="java.util.ArrayList"&gt;
 *          &lt;structure name="Person" map-as="person"/&gt;    &lt;!-- first occurences of all these objects --&gt;
 *      &lt;/collection&gt;
 *      &lt;structure name="EmployeeOfTheWeek"&gt;
 *          &lt;structure name="Person"&gt;
 *              <b>&lt;value name="idref" style="attribute" field="employeeOfTheWeek"
 *                serializer="org.dellroad.stuff.jibx.ParseUtil.serializeReference"
 *                deserializer="com.example.Employee.deserializeEmployeeReference"</b>/&gt;
 *          &lt;/structure&gt;
 *      &lt;/structure&gt;
 *  &lt;/mapping&gt;
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * If you want the reference to be optionally <code>null</code>, then you'll also need to add a <code>test-method</code>:
 * <blockquote><pre>
 *      <b>private boolean hasEmployeeOfTheWeek() {
 *          return this.getEmployeeOfTheWeek() != null;
 *      }</b>
 *
 *      &lt;structure name="EmployeeOfTheWeek" <b>usage="optional" test-method="hasEmployeeOfTheWeek"</b>&gt;
 *          &lt;structure name="Person"&gt;
 *              &lt;value name="idref" style="attribute" field="employeeOfTheWeek"
 *                serializer="org.dellroad.stuff.jibx.ParseUtil.serializeReference"
 *                deserializer="com.example.Employee.deserializeEmployeeReference"/&gt;
 *          &lt;/structure&gt;
 *      &lt;/structure&gt;
 * </pre></blockquote>
 * This approach causes the whole <code>&lt;EmployeeOfTheWeek&gt;</code> element to disappear when there is no
 * such employee. Alternately, you can avoid the need for the <code>test-method</code> if you want to allow
 * just the attribute to disappear, or you could even change from <code>style="attribute"</code> to <code>style="element"</code>;
 * in both cases you would be making the reference itself optional instead of the containing element.
 * </p>
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
     * Get the unique ID value for the given object.
     *
     * <p>
     * The implementation in {@link IdMapper} formats an ID of the form <code>N012345</code>
     * using the {@link IdGenerator} acquired from {@link IdGenerator#get}.
     *
     * @param obj any object
     * @return unique ID for the object
     */
    public static String getId(Object obj) {
        return IdMapper.formatId(IdGenerator.get().getId(obj));
    }

    /**
     * Set the unique ID value for the given object.
     *
     * <p>
     * The implementation in {@link IdMapper} expects an ID of the form <code>N012345</code>,
     * then associates the parsed {@code long} value with the given object
     * using the {@link IdGenerator} acquired from {@link IdGenerator#get}.
     *
     * @param obj object to register
     * @param idref string ID assigned to the object
     * @throws IllegalArgumentException if {@code idref} is not of the form <code>N012345</code>
     * @throws IllegalArgumentException if {@code idref} is already associated with a different object
     */
    public static void setId(Object obj, String idref) {
        IdGenerator.get().setId(obj, IdMapper.parseId(idref));
    }

    /**
     * Format the unique ID.
     *
     * @param id ID value
     * @return formatted idref
     */
    public static String formatId(long id) {
        return String.format("N%05d", id);
    }

    /**
     * Parse the unique ID value assigned to the given object by {@link #getId getId()}.
     *
     * @param idref ID value assigned to the object
     * @return parse ID number
     * @throws IllegalArgumentException if {@code idref} is not of the form <code>N012345</code>
     */
    public static long parseId(String idref) {
        if (idref == null || idref.length() == 0 || !idref.matches("N-?\\d+"))
            throw new IllegalArgumentException("invalid id value `" + idref + "'");
        long id;
        try {
            return Long.parseLong(idref.substring(1), 10);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid id value `" + idref + "'");
        }
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

