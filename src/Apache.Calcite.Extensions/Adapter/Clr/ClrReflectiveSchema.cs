using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;

using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// A schema whose tables are the sequences a .NET object exposes.
    /// </summary>
    /// <remarks>
    /// <c>ReflectiveSchema</c>, for .NET. The same idea — hand it an object, its members become tables — and
    /// three differences forced by what IKVM shows Java of a .NET type, each measured rather than assumed:
    ///
    /// <para>Its members are properties as well as fields, and a property is invisible to Java. So the walk is
    /// <see cref="ClrReflect"/>'s rather than <c>getFields()</c>.</para>
    ///
    /// <para>A sequence is an <see cref="IEnumerable{T}"/>, which is <b>not</b> a <c>java.lang.Iterable</c> —
    /// measured — so <c>ReflectiveSchema.getElementType</c> would refuse a <c>List&lt;Employee&gt;</c>
    /// outright. Here it is the candidate that matters, and an array is one too, as it is there.</para>
    ///
    /// <para>The element type is read off the sequence rather than being <c>Object.class</c>. Java erases it;
    /// .NET does not, so a table knows what its rows are without being told.</para>
    ///
    /// <para>Which of the two tables a member gets is <see cref="ClrTypeMapper.IsObjectRow"/>'s answer and not
    /// a setting: a type Calcite can read as it stands and can build back keeps its objects, and anything
    /// else is projected into columns. So a positional record round-trips as instances and a class of
    /// settable properties or of <see cref="decimal"/>s still queries, one row at a time.</para>
    /// </remarks>
    public class ClrReflectiveSchema : AbstractSchema
    {

        readonly object target;
        java.util.Map? tables;
        com.google.common.collect.Multimap? functions;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="target">The object whose members become the tables of this schema.</param>
        public ClrReflectiveSchema(object target)
        {
            this.target = target ?? throw new ArgumentNullException(nameof(target));
        }

        /// <summary>
        /// The object this schema reflects over.
        /// </summary>
        public object Target => target;

        /// <inheritdoc />
        protected override java.util.Map getTableMap() => tables ??= CreateTableMap();

        /// <inheritdoc />
        protected override com.google.common.collect.Multimap getFunctionMultimap() => functions ??= CreateFunctionMultimap();

        const BindingFlags Instance = BindingFlags.Public | BindingFlags.Instance;

        /// <summary>
        /// Builds the tables from the members of the target.
        /// </summary>
        /// <returns></returns>
        java.util.Map CreateTableMap()
        {
            var map = new java.util.LinkedHashMap();

            foreach (var property in target.GetType().GetProperties(Instance))
                if (property.GetMethod is { IsPublic: true } && property.GetIndexParameters().Length == 0)
                    Add(map, property.Name, property.PropertyType, () => property.GetValue(target));

            foreach (var field in target.GetType().GetFields(Instance))
                Add(map, field.Name, field.FieldType, () => field.GetValue(target));

            return map;
        }

        /// <summary>
        /// Builds the table macros from the methods of the target.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>ReflectiveSchema.createFunctionMap</c>, which takes every method returning a
        /// <c>TranslatableTable</c>. Here it is every method returning a <see cref="Table"/>, both of this
        /// adapter's being one, and a method returning a sequence is <em>not</em> among them: that would be a
        /// table function rather than a macro, and it would have to answer its row type without being called.
        /// </remarks>
        com.google.common.collect.Multimap CreateFunctionMultimap()
        {
            var builder = com.google.common.collect.ImmutableMultimap.builder();

            foreach (var method in target.GetType().GetMethods(Instance))
            {
                if (method.DeclaringType == typeof(object) || method.IsSpecialName)
                    continue;
                if (typeof(Table).IsAssignableFrom(method.ReturnType) == false)
                    continue;

                builder.put(method.Name, new ClrTableMacro(target, method));
            }

            return builder.build();
        }

        /// <summary>
        /// Adds a table for one member, where the member is a sequence of something with columns.
        /// </summary>
        /// <param name="map"></param>
        /// <param name="name"></param>
        /// <param name="type"></param>
        /// <param name="read"></param>
        /// <remarks>
        /// A member that is not a sequence is not a table and is passed over in silence, as
        /// <c>fieldRelation</c> passes over a field that is not a collection. A member that is a sequence of
        /// something with no columns at all is passed over too — <see cref="string"/> is the case that
        /// matters, being a sequence of <see cref="char"/> — and a sequence of something whose members cannot
        /// be represented becomes a table that refuses by name when its row type is asked for.
        /// </remarks>
        static void Add(java.util.Map map, string name, Type type, Func<object?> read)
        {
            var elementType = ElementTypeOf(type);
            if (elementType == null || ClrReflect.Members(elementType).Count == 0)
                return;

            var value = read();
            if (value == null)
                throw new InvalidOperationException($"'{name}' is null, so there is no table to read.");

            map.put(name, Of(elementType, value));
        }

        /// <summary>
        /// Returns the table a sequence of a given element type gets.
        /// </summary>
        /// <param name="elementType"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        public static Table Of(Type elementType, object source)
        {
            ArgumentNullException.ThrowIfNull(elementType);
            ArgumentNullException.ThrowIfNull(source);

            return ClrTypeMapper.IsObjectRow(elementType)
                ? new ClrObjectTable(elementType, source)
                : new ClrProjectingTable(elementType, (IEnumerable)source);
        }

        /// <summary>
        /// Returns what a sequence type yields, or null where the type is not a sequence.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>ReflectiveSchema.getElementType</c>. An array answers with its element type there and here
        /// alike; where it answers <c>Object.class</c> for an <c>Iterable</c>, having nothing else to say, an
        /// <see cref="IEnumerable{T}"/> states its own.
        /// </remarks>
        static Type? ElementTypeOf(Type type)
        {
            if (type == typeof(string))
                return null;

            if (type.IsArray)
                return type.GetElementType();

            foreach (var iface in type.IsInterface ? [type, .. type.GetInterfaces()] : type.GetInterfaces())
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    return iface.GetGenericArguments()[0];

            return null;
        }

        /// <inheritdoc />
        public override string toString() => $"ClrReflectiveSchema(target={target})";

    }

}
