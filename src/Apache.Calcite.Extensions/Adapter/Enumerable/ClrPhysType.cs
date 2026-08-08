using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Physical type of a row of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <see cref="PhysType"/>, member for member, answering in <see cref="Expression"/> where Calcite answers
    /// in linq4j. It is not a <see cref="PhysType"/> and does not derive from one: <c>PhysType</c> is
    /// <c>EnumerableConvention</c>'s row abstraction, its members are declared to return linq4j, and a node of
    /// this convention has no use for that answer. A node is in one convention or the other, and so is its
    /// physical type.
    ///
    /// <para>A <see cref="ClrPhysType"/> is the same triple a <c>PhysTypeImpl</c> is — a type factory, a
    /// <see cref="RelDataType"/>, and a <see cref="JavaRowFormat"/> that has already been through
    /// <c>JavaRowFormat.optimize</c>. Nothing of Calcite's is held: the two Rex entry points that are
    /// parameterised by a <c>PhysType</c> build one from those three where they are called, which is what
    /// that code already does.</para>
    ///
    /// <para>The format is still Calcite's enum, and <see cref="JavaRowFormatExtensions"/> answers for it the two
    /// members that would otherwise return linq4j. A row format of this convention's own is a separate
    /// pass.</para>
    ///
    /// </remarks>
    public interface ClrPhysType
    {

        /// <summary>
        /// Returns the CLR type that represents a row.
        /// </summary>
        /// <remarks>
        /// <c>PhysType.getRowType</c>, <b>boxed</b>, which is where this diverges from Calcite. The physical
        /// type of a one column row of <c>INTEGER NOT NULL</c> is <c>java.lang.Integer</c>, not
        /// <see cref="int"/>: a CLR sequence states its element type and nothing autoboxes at the boundary,
        /// so the choice is made once here rather than at each call site.
        ///
        /// <para>Calcite can leave it to its callers, because there is no <c>Enumerable&lt;int&gt;</c> in
        /// Java — the element is a reference whatever the physical type says, and javac inserts the
        /// conversion. So <c>joinSelector</c> and <c>generateComparator</c> each write
        /// <c>Primitive.box(physType.getRowType())</c> where they need it. Here
        /// <see cref="ClrPhysTypeImpl"/> boxes in its constructor and this is that;
        /// <c>ClrEnumerableRelImplementor.Result</c> refuses a sequence that disagrees.</para>
        /// </remarks>
        Type RowType { get; }

        /// <summary>
        /// Returns the CLR type used to store the field with the given ordinal.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.getFieldType</c>. For a row of <see cref="JavaRowFormat.ARRAY"/> the answer is
        /// <see cref="object"/> even where the field is not nullable, because that is what the array holds.
        /// </remarks>
        Type FieldType(int field);

        /// <summary>
        /// Returns the type factory.
        /// </summary>
        /// <remarks>
        /// <c>PhysType.getTypeFactory</c>.
        /// </remarks>
        JavaTypeFactory TypeFactory { get; }

        /// <summary>
        /// Returns the physical type of a field.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.field</c>.
        /// </remarks>
        ClrPhysType Field(int ordinal);

        /// <summary>
        /// Returns the physical type of a given field's component type.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.component</c>.
        /// </remarks>
        ClrPhysType Component(int field);

        /// <summary>
        /// Returns the SQL row type.
        /// </summary>
        /// <remarks>
        /// <c>PhysType.getRowType</c>.
        /// </remarks>
        RelDataType RelRowType { get; }

        /// <summary>
        /// Returns the CLR class of the field with the given ordinal.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.fieldClass</c>. The field's own type, as against <see cref="FieldType"/>, which is how
        /// the row stores it.
        /// </remarks>
        Type FieldClass(int field);

        /// <summary>
        /// Returns whether a given field allows null values.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.fieldNullable</c>.
        /// </remarks>
        bool FieldNullable(int index);

        /// <summary>
        /// Returns an expression reading a given field of an expression.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.fieldReference</c>.
        /// </remarks>
        Expression FieldReference(Expression expression, int field);

        /// <summary>
        /// Returns an expression reading a given field of an expression, as the given storage type.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="field"></param>
        /// <param name="storageType">The type the value is wanted as, or null for whatever the row holds.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.fieldReference</c>, the overload that optimises for the target storage type.
        /// </remarks>
        Expression FieldReference(Expression expression, int field, Type? storageType);

        /// <summary>
        /// Returns a lambda reading a list of fields into a comparable list.
        /// </summary>
        /// <param name="fields"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateAccessor</c>. The result is a <c>FlatLists</c> list, which is comparable and
        /// hashes by value, because it is what keys a lookup.
        /// </remarks>
        LambdaExpression GenerateAccessor(java.util.List fields);

        /// <summary>
        /// Returns a lambda reading a list of fields, yielding null where any of them is null.
        /// </summary>
        /// <param name="fields"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateAccessorWithoutNulls</c>. A key of two or more fields is null as a whole where
        /// any field of it is null, so a row with a null in its key matches nothing rather than matching
        /// another row with a null in the same place.
        /// </remarks>
        LambdaExpression GenerateAccessorWithoutNulls(java.util.List fields);

        /// <summary>
        /// Returns a lambda reading a list of fields, yielding null only where a field that is not null-safe is
        /// null.
        /// </summary>
        /// <param name="fields"></param>
        /// <param name="nullExclusionFlags"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateNullAwareAccessor</c>.
        /// </remarks>
        LambdaExpression GenerateNullAwareAccessor(java.util.List fields, java.util.List nullExclusionFlags);

        /// <summary>
        /// Returns a lambda selecting the given fields, in this type's own row format.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateSelector</c>.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields);

        /// <summary>
        /// Returns a lambda selecting the given fields, in the given row format.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fields"></param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateSelector</c>.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns a lambda selecting the given fields with an indicator per field, in the given row format.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fields"></param>
        /// <param name="usedFields">A subset of <paramref name="fields"/>; a field outside it is left at its
        /// default and its indicator set, which is what makes <c>GROUPING(field)</c> answer 1.</param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateSelector</c>, the grouping set overload.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, java.util.List usedFields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns the row type and the field expressions a selector for the given fields would be built from.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fields"></param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.selector</c>, which Calcite documents as used only by <c>EnumerableWindow</c>, and which
        /// is used only by <see cref="ClrEnumerableWindow"/> here for the same reason: a window builds the
        /// partition key a field at a time rather than taking the lambda whole.
        /// </remarks>
        (Type RowType, IReadOnlyList<Expression> Expressions) Selector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns the physical type of the given fields of this one, in the given row format.
        /// </summary>
        /// <param name="integers"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.project</c>. The output format is optimised where there are no fields or one.
        /// </remarks>
        ClrPhysType Project(java.util.List integers, JavaRowFormat format);

        /// <summary>
        /// Returns the physical type of the given fields of this one, optionally with indicator fields, in the
        /// given row format.
        /// </summary>
        /// <param name="integers"></param>
        /// <param name="indicator"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.project</c>.
        /// </remarks>
        ClrPhysType Project(java.util.List integers, bool indicator, JavaRowFormat format);

        /// <summary>
        /// Returns a lambda yielding a collation key, and the comparator ordering two of those keys.
        /// </summary>
        /// <param name="collations"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateCollationKey</c>. The comparator is sometimes null, which is Calcite's way of
        /// saying the key orders itself.
        /// </remarks>
        (LambdaExpression Selector, Expression? Comparator) GenerateCollationKey(java.util.List collations);

        /// <summary>
        /// Returns the comparator ordering two whole rows of this type.
        /// </summary>
        /// <param name="collation"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateComparator</c>. Unlike the comparator of
        /// <see cref="GenerateCollationKey"/>, this one acts on the whole element.
        /// </remarks>
        Expression GenerateComparator(RelCollation collation);

        /// <summary>
        /// Returns the comparator a merge join orders its inputs by.
        /// </summary>
        /// <param name="collation"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.generateMergeJoinComparator</c>, which differs from
        /// <see cref="GenerateComparator"/> in refusing to call two nulls equal.
        /// </remarks>
        Expression GenerateMergeJoinComparator(RelCollation collation);

        /// <summary>
        /// Returns the expression yielding a comparer for rows of this type, or null where a row compares
        /// itself.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.comparer</c>. A row of <see cref="JavaRowFormat.ARRAY"/> is an array, whose own equality
        /// is by reference, so a set operation over one is wrong without this.
        /// </remarks>
        Expression? Comparer();

        /// <summary>
        /// Returns an expression building a row of this type from one expression per field.
        /// </summary>
        /// <param name="expressions"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.record</c>.
        /// </remarks>
        Expression Record(IReadOnlyList<Expression> expressions);

        /// <summary>
        /// Returns the row format.
        /// </summary>
        /// <remarks>
        /// <c>PhysType.getFormat</c>, and already optimised.
        /// </remarks>
        JavaRowFormat Format { get; }

        /// <summary>
        /// Returns one expression per named field, each at the field's own type.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="argList"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.accessors</c>.
        /// </remarks>
        IReadOnlyList<Expression> Accessors(Expression parameter, java.util.List argList);

        /// <summary>
        /// Returns a copy of this type that allows nulls where <paramref name="nullable"/> is set.
        /// </summary>
        /// <param name="nullable"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.makeNullable</c>.
        /// </remarks>
        ClrPhysType MakeNullable(bool nullable);

        /// <summary>
        /// Converts a sequence of this physical type to one whose rows use the given physical type.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="targetPhysType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.convertTo</c>, which Calcite deprecates for the same reason it gives: only the row
        /// format of the expression is affected, so the second parameter is misleading and asks a caller for a
        /// whole physical type it does not need. Kept because Calcite keeps it.
        /// </remarks>
        [Obsolete("Use the JavaRowFormat overload; only the row format of the expression is affected.")]
        Expression ConvertTo(Expression expression, ClrPhysType targetPhysType);

        /// <summary>
        /// Converts a sequence of this physical type to one whose rows use the given row format.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysType.convertTo</c>.
        /// </remarks>
        Expression ConvertTo(Expression expression, JavaRowFormat targetFormat);

        /// <summary>
        /// Converts an asynchronous sequence of this physical type to one whose rows use the given row
        /// format.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="ConvertTo(Expression, JavaRowFormat)"/> for a plan of the asynchronous convention, and
        /// the one member of this type that has two of itself.
        ///
        /// <para>Calcite has no counterpart, so this is a divergence, and an additive one: <c>convertTo</c>
        /// is the only member of <c>PhysType</c> that takes a <em>sequence</em> rather than a row, so it is
        /// the only one a second convention cannot share. Moving it out to make this type purely row-shaped
        /// would have been the smaller change and is not made, because this type mirrors <c>PhysType</c>
        /// member for member and a member that is there because Calcite put it there stays.</para>
        /// </remarks>
        Expression ConvertToAsync(Expression expression, JavaRowFormat targetFormat);

    }

}
