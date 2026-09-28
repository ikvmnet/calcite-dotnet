using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Physical type of a row of the <see cref="Cursor.ClrCursorConvention"/> calling convention: how a row
    /// with a given relational type is represented at run time, and how to build expressions over it.
    /// </summary>
    /// <remarks>
    /// Mirrors <see cref="PhysType"/> member for member, returning <see cref="System.Linq.Expressions"/>
    /// expressions where Calcite returns linq4j ones. It does not implement <see cref="PhysType"/>. Like a
    /// <c>PhysTypeImpl</c>, it is defined by a type factory, a <see cref="RelDataType"/> and a
    /// <see cref="JavaRowFormat"/>; code that needs a Calcite <c>PhysType</c> builds one from those three.
    /// </remarks>
    public interface ClrPhysType
    {

        /// <summary>
        /// Gets the CLR type of a row, boxed.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>PhysType.getJavaRowType</c>, but boxed: a one-column <c>INTEGER NOT NULL</c> row is
        /// <c>java.lang.Integer</c>, not <see cref="int"/>. A cursor's element type is this type, and
        /// <see cref="Cursor.ClrCursorRelImplementor.Result"/> refuses a cursor of any other.
        /// </remarks>
        Type RowType { get; }

        /// <summary>
        /// Gets the Java type of a row as the type factory gives it, not boxed.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>PhysType.getJavaRowType</c>.
        /// </remarks>
        java.lang.reflect.Type JavaRowType { get; }

        /// <summary>
        /// Returns the CLR type in which the row stores a field.
        /// </summary>
        /// <param name="field">The field ordinal.</param>
        /// <returns>The storage type; for a <see cref="JavaRowFormat.ARRAY"/> row this is <see cref="object"/>
        /// even for a field that is not nullable.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.getJavaFieldType</c>.
        /// </remarks>
        Type FieldType(int field);

        /// <summary>
        /// Gets the type factory.
        /// </summary>
        JavaTypeFactory TypeFactory { get; }

        /// <summary>
        /// Returns the physical type of a field, as a one-field row if the field is not a struct.
        /// </summary>
        /// <param name="ordinal">The field ordinal.</param>
        /// <returns>The field's physical type, in this type's format.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.field</c>.
        /// </remarks>
        ClrPhysType Field(int ordinal);

        /// <summary>
        /// Returns the physical type of a collection field's component type, as a one-field row if the
        /// component is not a struct.
        /// </summary>
        /// <param name="field">The field ordinal.</param>
        /// <returns>The component's physical type, in this type's format.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.component</c>.
        /// </remarks>
        ClrPhysType Component(int field);

        /// <summary>
        /// Gets the relational row type.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>PhysType.getRowType</c>.
        /// </remarks>
        RelDataType RelRowType { get; }

        /// <summary>
        /// Returns the CLR class of a field's values, as distinct from <see cref="FieldType"/>, the type the
        /// row stores it in.
        /// </summary>
        /// <param name="field">The field ordinal.</param>
        /// <returns>The field's class; <c>Object[]</c> for a type the type factory gives no class.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.fieldClass</c>.
        /// </remarks>
        Type FieldClass(int field);

        /// <summary>
        /// Returns whether a field is nullable.
        /// </summary>
        /// <param name="index">The field ordinal.</param>
        /// <returns><see langword="true"/> if the field's type is nullable.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.fieldNullable</c>.
        /// </remarks>
        bool FieldNullable(int index);

        /// <summary>
        /// Returns an expression reading a field of a row, as the field's class.
        /// </summary>
        /// <param name="expression">An expression whose value is a row of this type.</param>
        /// <param name="field">The field ordinal.</param>
        /// <returns>The field read.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.fieldReference</c>.
        /// </remarks>
        Expression FieldReference(Expression expression, int field);

        /// <summary>
        /// Returns an expression reading a field of a row, as a given storage type.
        /// </summary>
        /// <param name="expression">An expression whose value is a row of this type.</param>
        /// <param name="field">The field ordinal.</param>
        /// <param name="storageType">The type wanted, or <see langword="null"/> for the field's class. A date,
        /// time or timestamp field can be read as its internal <see cref="int"/> or <see cref="long"/>.</param>
        /// <returns>The field read.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.fieldReference</c>, the overload that takes a storage type.
        /// </remarks>
        Expression FieldReference(Expression expression, int field, Type? storageType);

        /// <summary>
        /// Returns a lambda that extracts a key from a row: nothing, one field's value, or a comparable list of
        /// several fields.
        /// </summary>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <returns>The key selector.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateAccessor</c>. With no fields the key is an empty comparable list; with
        /// several it is a <c>FlatLists</c> list, which compares and hashes by value.
        /// </remarks>
        LambdaExpression GenerateAccessor(java.util.List fields);

        /// <summary>
        /// Returns a lambda that extracts a key from a row, as <see cref="GenerateAccessor"/> does, but yields
        /// null for a key of two or more fields if any of them is null.
        /// </summary>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <returns>The key selector.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateAccessorWithoutNulls</c>. A null key matches nothing, so a row with a
        /// null in its key does not join to another with a null in the same place.
        /// </remarks>
        LambdaExpression GenerateAccessorWithoutNulls(java.util.List fields);

        /// <summary>
        /// Returns a lambda that extracts a key from a row as a list, even for one field, yielding null if a
        /// field flagged for null exclusion is null or, for a struct field, holds a null at any depth.
        /// </summary>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <param name="nullExclusionFlags">One boolean per field: <see langword="true"/> where a null makes
        /// the key null (<c>=</c>), <see langword="false"/> where nulls compare equal
        /// (<c>IS NOT DISTINCT FROM</c>).</param>
        /// <returns>The key selector.</returns>
        /// <exception cref="java.lang.AssertionError">The two lists differ in length.</exception>
        /// <remarks>
        /// Mirrors <c>PhysType.generateNullAwareAccessor</c>.
        /// </remarks>
        LambdaExpression GenerateNullAwareAccessor(java.util.List fields, java.util.List nullExclusionFlags);

        /// <summary>
        /// Returns a lambda selecting fields of a row into a row of this type's format.
        /// </summary>
        /// <param name="parameter">The lambda's parameter, of this type's <see cref="RowType"/>.</param>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <returns>The selector.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateSelector</c>.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields);

        /// <summary>
        /// Returns a lambda selecting fields of a row into a row of the given format.
        /// </summary>
        /// <param name="parameter">The lambda's parameter, of this type's <see cref="RowType"/>.</param>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <param name="targetFormat">The output format; replaced by <see cref="JavaRowFormat.LIST"/> for no
        /// fields and <see cref="JavaRowFormat.SCALAR"/> for one.</param>
        /// <returns>The selector; the identity if this type's format is <see cref="JavaRowFormat.SCALAR"/>.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateSelector</c>.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns a lambda selecting fields of a row for a grouping set, followed by one indicator per field.
        /// </summary>
        /// <param name="parameter">The lambda's parameter, of this type's <see cref="RowType"/>.</param>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <param name="usedFields">The subset of <paramref name="fields"/> the grouping set groups by. Any
        /// other field is output as its type's default value with its indicator <see langword="true"/>, which is
        /// what makes <c>GROUPING(field)</c> return 1.</param>
        /// <param name="targetFormat">The output format.</param>
        /// <returns>The selector.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateSelector</c>, the overload for grouping sets.
        /// </remarks>
        LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, java.util.List usedFields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns the output row type and field expressions from which a selector for the given fields would
        /// be built.
        /// </summary>
        /// <param name="parameter">The row parameter, of this type's <see cref="RowType"/>.</param>
        /// <param name="fields">The field ordinals, a list of integers.</param>
        /// <param name="targetFormat">The output format, adjusted as in
        /// <see cref="GenerateSelector(ParameterExpression, java.util.List, JavaRowFormat)"/>.</param>
        /// <returns>The output row type and one expression per field.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.selector</c>, which Calcite uses only in <c>EnumerableWindow</c>; this is used
        /// by <see cref="Cursor.ClrCursorWindow"/>.
        /// </remarks>
        (Type RowType, IReadOnlyList<Expression> Expressions) Selector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat);

        /// <summary>
        /// Returns the physical type of a projection of this type's fields.
        /// </summary>
        /// <param name="integers">The field ordinals, a list of integers.</param>
        /// <param name="format">The output format, optimized for the projected row type.</param>
        /// <returns>The projected physical type.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.project</c>.
        /// </remarks>
        ClrPhysType Project(java.util.List integers, JavaRowFormat format);

        /// <summary>
        /// Returns the physical type of a projection of this type's fields, optionally followed by a
        /// non-nullable <c>BOOLEAN</c> indicator field per projected field.
        /// </summary>
        /// <param name="integers">The field ordinals, a list of integers.</param>
        /// <param name="indicator">Whether to add indicator fields, each named <c>i$</c> and the field's name.</param>
        /// <param name="format">The output format, optimized for the projected row type.</param>
        /// <returns>The projected physical type.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.project</c>.
        /// </remarks>
        ClrPhysType Project(java.util.List integers, bool indicator, JavaRowFormat format);

        /// <summary>
        /// Returns a key selector and a comparator that together sort rows by a list of field collations.
        /// </summary>
        /// <param name="collations">The field collations, a list of <see cref="RelFieldCollation"/>.</param>
        /// <returns>For one collation, a selector of that field and a null-aware comparator of its values;
        /// otherwise the identity selector and a comparator of whole rows.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateCollationKey</c>.
        /// </remarks>
        (LambdaExpression Selector, Expression? Comparator) GenerateCollationKey(java.util.List collations);

        /// <summary>
        /// Returns an expression creating a <c>java.util.Comparator</c> of whole rows by a collation.
        /// </summary>
        /// <param name="collation">The collation.</param>
        /// <returns>The comparator, which takes boxed rows.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.generateComparator</c>.
        /// </remarks>
        Expression GenerateComparator(RelCollation collation);

        /// <summary>
        /// Returns an expression creating the comparator a merge join orders its keys by.
        /// </summary>
        /// <param name="collation">The key collation, which must be ascending with nulls last on every field.</param>
        /// <returns>The comparator, which takes boxed rows.</returns>
        /// <exception cref="java.lang.AssertionError">A field collation is not ascending with nulls last.</exception>
        /// <remarks>
        /// Mirrors <c>PhysType.generateMergeJoinComparator</c>. Unlike <see cref="GenerateComparator"/>, it
        /// does not treat two nulls as equal.
        /// </remarks>
        Expression GenerateMergeJoinComparator(RelCollation collation);

        /// <summary>
        /// Returns an expression creating the equality comparer for rows of this type, or
        /// <see langword="null"/> if rows compare correctly by their own equality.
        /// </summary>
        /// <returns>The comparer expression, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.comparer</c>. An array row needs a comparer because arrays compare by reference,
        /// and a row holding a struct at any depth needs a deep comparer.
        /// </remarks>
        Expression? Comparer();

        /// <summary>
        /// Returns an expression creating a row of this type from one expression per field.
        /// </summary>
        /// <param name="expressions">The field values, in field order.</param>
        /// <returns>The row.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.record</c>.
        /// </remarks>
        Expression Record(IReadOnlyList<Expression> expressions);

        /// <summary>
        /// Gets the row format.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>PhysType.getFormat</c>.
        /// </remarks>
        JavaRowFormat Format { get; }

        /// <summary>
        /// Returns one expression per listed field, each converted to the field's class.
        /// </summary>
        /// <param name="parameter">An expression whose value is a row of this type.</param>
        /// <param name="argList">The field ordinals, a list of integers.</param>
        /// <returns>The field reads.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.accessors</c>.
        /// </remarks>
        IReadOnlyList<Expression> Accessors(Expression parameter, java.util.List argList);

        /// <summary>
        /// Returns this type with a nullable row type and a boxed row class, or this type unchanged if
        /// <paramref name="nullable"/> is <see langword="false"/>.
        /// </summary>
        /// <param name="nullable">Whether to make the type nullable.</param>
        /// <returns>The physical type.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.makeNullable</c>.
        /// </remarks>
        ClrPhysType MakeNullable(bool nullable);

        /// <summary>
        /// Converts a synchronous open of this type's rows to one yielding rows in another physical type's
        /// format.
        /// </summary>
        /// <param name="expression">The synchronous open.</param>
        /// <param name="targetPhysType">The physical type whose format the rows are converted to.</param>
        /// <returns>The converted open.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.convertTo(Expression, PhysType)</c>, which Calcite also deprecates: only the
        /// target's format is used.
        /// </remarks>
        [Obsolete("Use the JavaRowFormat overload; only the row format of the expression is affected.")]
        Expression ConvertTo(Expression expression, ClrPhysType targetPhysType);

        /// <summary>
        /// Converts a synchronous open of this type's rows to one yielding rows in the given format.
        /// </summary>
        /// <param name="expression">The synchronous open.</param>
        /// <param name="targetFormat">The target format, used as given rather than optimized.</param>
        /// <returns>The converted open, or <paramref name="expression"/> if the format is unchanged.</returns>
        /// <remarks>
        /// Mirrors <c>PhysType.convertTo</c>, over a cursor rather than an <c>Enumerable</c>.
        /// </remarks>
        Expression ConvertTo(Expression expression, JavaRowFormat targetFormat);

        /// <summary>
        /// Converts an awaiting open of this type's rows to one yielding rows in the given format.
        /// </summary>
        /// <param name="implementor">The implementor, which supplies the cancellation token parameter.</param>
        /// <param name="expression">The awaiting open.</param>
        /// <param name="targetFormat">The target format, used as given rather than optimized.</param>
        /// <returns>The converted open, or <paramref name="expression"/> if the format is unchanged.</returns>
        /// <remarks>
        /// The awaiting counterpart of <see cref="ConvertTo(Expression, JavaRowFormat)"/>; Calcite has none.
        /// </remarks>
        Expression ConvertToAsync(Cursor.ClrCursorRelImplementor implementor, Expression expression, JavaRowFormat targetFormat);

    }

}
