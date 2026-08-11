using System;

using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica.util;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// The relational type of a CLR type.
    /// </summary>
    /// <remarks>
    /// The row type is the one thing here that is not already in the CLR type: names, SQL types, nullability
    /// and order are the schema, and <c>typeof(Employee)</c> says a member is a <see cref="decimal"/> without
    /// saying it is the third column of a <c>DECIMAL(19, 6)</c>. Calcite requires it and .NET has no
    /// equivalent, so it is built.
    ///
    /// <para>A member Calcite reads as it stands goes through <c>JavaTypeFactory.createType</c>, exactly as
    /// <c>createStructType(Class)</c> sends a field's through it; only the walk that finds the members
    /// differs. A member it does not is given a SQL type here and converted per value by
    /// <see cref="ClrRowConverter"/> — and that pair is what decides which table a schema builds, because a
    /// row that is the object itself has nowhere to put a conversion.</para>
    /// </remarks>
    public static class ClrTypeMapper
    {

        /// <summary>
        /// Returns the row type of a table whose rows are instances of <paramref name="type"/>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// A <see cref="ClrRowType"/>, which carries the class so that <c>getJavaClass</c> answers with it,
        /// and the record type so that a member can be reached by ordinal. Only for a type every member of
        /// which Calcite reads as it stands — <see cref="IsObjectRow"/> is the question, and
        /// <see cref="ColumnRowType"/> is the answer for everything else.
        /// </remarks>
        public static RelDataType RowType(JavaTypeFactory typeFactory, Type type)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(type);

            var members = ClrReflect.Members(type);
            var fields = new java.util.ArrayList(members.Count);
            for (int i = 0; i < members.Count; i++)
            {
                var member = members[i];
                if (IsNativelyRepresented(member.Type) == false)
                    throw new NotSupportedException(
                        $"'{type}.{member.Member.Name}' is a '{member.Type}', which Calcite has no representation for, " +
                        $"so a row of '{type}' cannot be the object itself.");

                fields.add(new RelDataTypeFieldImpl(member.Member.Name, i, typeFactory.createType((java.lang.Class)member.Type)));
            }

            return new ClrRowType(fields, (java.lang.Class)type, ClrRecordType.Of(type));
        }

        /// <summary>
        /// Returns the row type of a table that breaks instances of <paramref name="type"/> into columns.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// An ordinary <c>RelRecordType</c>, the rows being <c>object?[]</c> and the class having nothing to
        /// do with them. Every member goes through <see cref="ColumnType"/>, which refuses by name what it
        /// cannot convert.
        /// </remarks>
        public static RelDataType ColumnRowType(JavaTypeFactory typeFactory, Type type)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(type);

            var members = ClrReflect.Members(type);
            var builder = typeFactory.builder();
            for (int i = 0; i < members.Count; i++)
            {
                var member = members[i];
                builder.add(
                    member.Member.Name,
                    ColumnType(typeFactory, member.Type)
                        ?? throw new NotSupportedException($"'{type}.{member.Member.Name}' is a '{member.Type}', which has no SQL type."));
            }

            return builder.build();
        }

        /// <summary>
        /// Returns the SQL type of one column, or <see langword="null"/> where the CLR type has none.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// Each of these is paired with a method of <see cref="ClrValues"/>, and the pairing is what
        /// <c>JavaTypeFactoryImpl.getJavaClass</c> dictates rather than what the SQL type is called: a
        /// TIMESTAMP column holds a <see cref="long"/>, so the conversion answers one.
        /// </remarks>
        public static RelDataType? ColumnType(JavaTypeFactory typeFactory, Type type)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(type);

            // a nullable value type is the type it wraps, and the one place .NET says something Java cannot
            if (Nullable.GetUnderlyingType(type) is Type underlying)
                return ColumnType(typeFactory, underlying) is RelDataType inner
                    ? typeFactory.createTypeWithNullability(inner, true)
                    : null;

            if (IsNativelyRepresented(type))
                return typeFactory.createType((java.lang.Class)type);

            // an enum is its underlying integer. Calcite's SYMBOL is for its own enums and reaches no SQL
            if (type.IsEnum)
                return typeFactory.createType((java.lang.Class)Enum.GetUnderlyingType(type));

            if (type == typeof(decimal))
                return typeFactory.createSqlType(SqlTypeName.DECIMAL, ClrValues.DecimalPrecision, ClrValues.DecimalScale);

            if (type == typeof(DateTime))
                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

            if (type == typeof(DateOnly))
                return typeFactory.createSqlType(SqlTypeName.DATE);

            if (type == typeof(TimeOnly))
                return typeFactory.createSqlType(SqlTypeName.TIME);

            if (type == typeof(TimeSpan))
                return typeFactory.createSqlIntervalType(new SqlIntervalQualifier(TimeUnit.DAY, TimeUnit.SECOND, SqlParserPos.ZERO));

            if (type == typeof(Guid))
                return typeFactory.createSqlType(SqlTypeName.CHAR, 36);

            return null;
        }

        /// <summary>
        /// Returns whether a table over <paramref name="type"/> can keep its rows as the objects.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// Two conditions, and both are about what a row has to be able to do rather than about taste.
        ///
        /// <para>Every member must be one Calcite reads as it stands, because a field read of an object row
        /// is <c>Expression.Property</c> and there is nowhere to put a conversion.</para>
        ///
        /// <para>And the type must have a constructor taking every member, because
        /// <c>JavaRowFormat.CUSTOM.record</c> builds a row back with <c>new_(clazz, …)</c> — which any
        /// projection that keeps the row shape will do. A positional record has one; a type of settable
        /// properties does not, and gets a table that projects rather than a failure at plan time.</para>
        /// </remarks>
        public static bool IsObjectRow(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            var members = ClrReflect.Members(type);
            if (members.Count == 0)
                return false;

            if (ClrReflect.Constructor(type) == null)
                return false;

            for (int i = 0; i < members.Count; i++)
                if (IsNativelyRepresented(members[i].Type) == false)
                    return false;

            return true;
        }

        /// <summary>
        /// Returns whether a value of a CLR type is one Calcite reads as it stands.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <b>Calcite's own test, not a table written again here.</b> <c>JavaTypeFactoryImpl.createType</c>
        /// answers a primitive or a box with <c>createJavaType</c>, answers anything
        /// <c>JavaToSqlTypeConversionRules</c> knows the same way, and sends everything else to
        /// <c>createStructType</c> — which over a .NET class is the silent zero-column row. Asking the same
        /// question here is what turns that silence into a refusal.
        ///
        /// <para>So <see cref="int"/>, <see cref="long"/>, <see cref="string"/> and the rest are in, IKVM
        /// showing them to Java as <c>int</c>, <c>long</c> and <c>java.lang.String</c>; so is a member whose
        /// type is a Java one already, a <c>BigDecimal</c> or a <c>java.sql.Timestamp</c>.
        /// <see cref="decimal"/>, <see cref="DateTime"/>, <see cref="Guid"/> and <c>int?</c> are out — they
        /// arrive as <c>cli.System.Decimal</c> and friends, which Calcite has never heard of — and each is
        /// converted per value instead, by <see cref="ColumnType"/> and <see cref="ClrValues"/>.</para>
        /// </remarks>
        public static bool IsNativelyRepresented(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            var clazz = (java.lang.Class)type;

            switch (J.Primitive.flavor(clazz).name())
            {
                case nameof(J.Primitive.Flavor.PRIMITIVE):
                case nameof(J.Primitive.Flavor.BOX):
                    return true;
            }

            return JavaToSqlTypeConversionRules.instance().lookup(clazz) != null;
        }

    }

}
