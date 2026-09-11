using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Types
{

    /// <summary>
    /// The mappings that hold without anyone registering anything.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The table says which CLR type pairs with which Calcite type and nothing else;
    /// <see cref="CalciteValues"/> says how a value crosses, in both directions, from the Calcite type the
    /// mapping carries. That split is why almost every entry names the same pair of functions: the
    /// conversion for a <c>DATE</c> is not a different function from the conversion for a <c>TIMESTAMP</c>,
    /// it is the same one told which type it is converting. The entries that do name something else are the
    /// ones whose CLR type is not what the conversion answers with by default — a <see cref="DateOnly"/>
    /// read out of a <c>TIMESTAMP</c>, say.
    /// </para>
    /// <para>
    /// The order of the table is its priority, so the first entry written for a Calcite type is what that
    /// type reads back as and the first written for a CLR type is what that type is written as. The two
    /// catch-alls at the end are what a type nobody has claimed falls to: they convert on the strength of
    /// the runtime class of the value rather than on either type, which is what the ADO.NET surface does
    /// for a column whose SQL type it has no case for.
    /// </para>
    /// <para>
    /// </para>
    /// <para>
    /// Every entry is a fact about Calcite rather than a preference. <c>FLOAT</c> is eight bytes here as it
    /// is in SQL and shares <c>DOUBLE</c>'s representation, <c>REAL</c> being the four-byte one —
    /// <c>JavaTypeFactoryImpl.getJavaClass</c> says so and marks it "sic". <c>TINYINT</c> is signed and the
    /// unsigned types are joou wrappers rather than wider <c>java.lang</c> ones. A <c>DATE</c> is a count of
    /// days and a <c>TIME</c> a count of milliseconds, both in an <c>Integer</c>; a <c>TIMESTAMP</c> is a
    /// count of milliseconds in a <c>Long</c>. A <c>UUID</c> is a <c>java.util.UUID</c> and not text.
    /// </para>
    /// </remarks>
    public sealed class DefaultClrTypeResolver : IClrTypeResolver
    {

        /// <summary>
        /// Gets the singleton instance.
        /// </summary>
        public static DefaultClrTypeResolver Instance { get; } = new DefaultClrTypeResolver();

        /// <summary>
        /// Converts a value to the representation the Calcite type it is being written as is held in.
        /// </summary>
        static object? To(object value, RelDataType relType) => CalciteValues.ToJava(value, relType);

        /// <summary>
        /// Converts a value from the representation the Calcite type it came out of is held in.
        /// </summary>
        static object? From(object value, RelDataType relType) => CalciteValues.ToClr(value, relType);

        readonly ClrTypeMappingCollection _mappings = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        DefaultClrTypeResolver()
        {
            var m = _mappings;

            // the natural pairs, each the default in both directions
            m.Add(typeof(bool), SqlTypeName.BOOLEAN, To, From);
            m.Add(typeof(sbyte), SqlTypeName.TINYINT, To, From);
            m.Add(typeof(short), SqlTypeName.SMALLINT, To, From);
            m.Add(typeof(int), SqlTypeName.INTEGER, To, From);
            m.Add(typeof(long), SqlTypeName.BIGINT, To, From);
            m.Add(typeof(byte), SqlTypeName.UTINYINT, To, From);
            m.Add(typeof(ushort), SqlTypeName.USMALLINT, To, From);
            m.Add(typeof(uint), SqlTypeName.UINTEGER, To, From);
            m.Add(typeof(ulong), SqlTypeName.UBIGINT, To, From);
            m.Add(typeof(float), SqlTypeName.REAL, To, From);
            m.Add(typeof(double), SqlTypeName.DOUBLE, To, From);
            m.Add(typeof(decimal), SqlTypeName.DECIMAL, To, From);
            m.Add(typeof(string), SqlTypeName.VARCHAR, To, From);
            m.Add(typeof(byte[]), SqlTypeName.VARBINARY, To, From);
            m.Add(typeof(Guid), SqlTypeName.UUID, To, From);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP, To, From);
            m.Add(typeof(TimeSpan), SqlTypeName.TIME, To, From);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_TZ, To, From);

            // what a Calcite type reads back as where the CLR type it pairs with is spoken for above
            m.Add(typeof(string), SqlTypeName.CHAR, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(byte[]), SqlTypeName.BINARY, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(double), SqlTypeName.FLOAT, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTime), SqlTypeName.DATE, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_TZ, To, From, ClrTypeMatch.RelDefault);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, To, From, ClrTypeMatch.RelDefault);

            // what a CLR type is written as where the Calcite type it pairs with is spoken for above
            m.Add(typeof(DateOnly), SqlTypeName.DATE, To, (v, t) => DateOnly.FromDateTime((DateTime)From(v, t)!), ClrTypeMatch.ClrDefault);
            m.Add(typeof(TimeOnly), SqlTypeName.TIME, To, (v, t) => TimeOnly.FromTimeSpan((TimeSpan)From(v, t)!), ClrTypeMatch.ClrDefault);

            // legal when asked for by name, and nobody's default. A Guid is a UUID and is what a caller
            // writing one means; a character column holding the text of one is a character column, and
            // reading it as a Guid is something the caller has to ask for. These two say the text
            // themselves because ToChar does not format a value that is not already a string
            m.Add(typeof(Guid), SqlTypeName.CHAR, static (v, _) => ((Guid)v).ToString(), (v, t) => Guid.Parse((string)From(v, t)!), ClrTypeMatch.Named, precision: 36);
            m.Add(typeof(Guid), SqlTypeName.VARCHAR, static (v, _) => ((Guid)v).ToString(), (v, t) => Guid.Parse((string)From(v, t)!), ClrTypeMatch.Named);
            m.Add(typeof(DateOnly), SqlTypeName.TIMESTAMP, To, (v, t) => DateOnly.FromDateTime((DateTime)From(v, t)!), ClrTypeMatch.Named);
            m.Add(typeof(TimeOnly), SqlTypeName.TIMESTAMP, To, (v, t) => TimeOnly.FromDateTime((DateTime)From(v, t)!), ClrTypeMatch.Named);
            m.Add(typeof(DateTime), SqlTypeName.TIMESTAMP_TZ, To, (v, t) => ((DateTimeOffset)From(v, t)!).UtcDateTime, ClrTypeMatch.Named);
            m.Add(typeof(DateTimeOffset), SqlTypeName.TIMESTAMP, To, (v, t) => new DateTimeOffset((DateTime)From(v, t)!, TimeSpan.Zero), ClrTypeMatch.Named);

            // an interval is a count, in the same two classes a DATE and a TIMESTAMP are: months in an
            // Integer for the year-month intervals, milliseconds in a Long for the day-time ones. One
            // entry each rather than thirteen, the type name being what tells them apart
            m.Add(typeof(int), SqlTypeName.INTERVAL_YEAR, To, From, ClrTypeMatch.RelDefault,
                relTypePredicate: static t => SqlTypeName.YEAR_INTERVAL_TYPES.contains(t.getSqlTypeName()));
            m.Add(typeof(long), SqlTypeName.INTERVAL_DAY, To, From, ClrTypeMatch.RelDefault,
                relTypePredicate: static t => SqlTypeName.DAY_INTERVAL_TYPES.contains(t.getSqlTypeName()));

            // the type whose only value is null. Reading one is null whatever a provider handed over, and
            // writing one is null whatever a caller wrote: java.lang.Void is what holds it and it has no
            // instances
            m.Add(typeof(object), SqlTypeName.NULL, static (_, _) => null, static (_, _) => null, ClrTypeMatch.RelDefault);

            // a Calcite type nothing above claimed: convert the value on the strength of its runtime class.
            // ANY, VARIANT and OTHER arrive here -- the first carrying no type, the second carrying its
            // payload's, the third being what a class the SQL type system has no name for becomes, so that
            // a column a schema typed with createJavaType already holds the class that schema chose.
            //
            // A type this project has no reading for -- a GEOMETRY, a SYMBOL, a CURSOR -- arrives here
            // too, and is refused where refusing is what matters. Reading one hands the caller the value
            // the column holds, which costs nothing; writing one is checked against
            // JavaTypeFactory.getJavaClass by ClrTypeMapping, so a Long offered for a GEOMETRY fails at
            // the boundary and names the type. That check is a better refusal than a list here would be,
            // because it is the type factory's own answer rather than a second opinion about it
            m.Add(
                typeof(object),
                SqlTypeName.ANY,
                To,
                From,
                ClrTypeMatch.RelDefault,
                clrTypePredicate: static t => t is null || t == typeof(object),
                relTypePredicate: static _ => true);

            // a CLR type nothing above claimed, which is what a caller binding a value of their own gets
            m.Add(
                typeof(object),
                SqlTypeName.ANY,
                To,
                From,
                ClrTypeMatch.ClrDefault,
                clrTypePredicate: static _ => true);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A collection and a row are answered ahead of the table, because the CLR type they are seen as is
        /// not a constant an entry could carry: an <c>INTEGER ARRAY</c> is an <see cref="int"/><c>[]</c>
        /// and a <c>VARCHAR ARRAY</c> a <see cref="string"/><c>[]</c>, which is the component's own answer
        /// with one dimension added. Composing through <see cref="ClrTypeContext.Registry"/> rather than
        /// recursing here is what makes that a caller's answer too, where the caller has claimed the
        /// component type.
        /// </remarks>
        public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
        {
            ArgumentNullException.ThrowIfNull(context);

            if (relType is not null && Structural(clrType, relType, context) is ClrTypeMapping structural)
                return structural;

            return _mappings.GetMapping(clrType, relType, context);
        }

        /// <summary>
        /// Answers for the types whose CLR type is built out of another type's.
        /// </summary>
        /// <param name="clrType"></param>
        /// <param name="relType"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        static ClrTypeMapping? Structural(Type? clrType, RelDataType relType, ClrTypeContext context)
        {
            // a row's fields are heterogeneous, so the array that carries them is of object however alike
            // they happen to be
            if (relType.isStruct())
                return clrType is null || clrType == typeof(object[]) || clrType == typeof(object)
                    ? new CompositeClrTypeMapping(context, relType, typeof(object[]))
                    : null;

            switch (relType.getSqlTypeName().name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    {
                        // the component's own answer with one dimension added; a component that holds a
                        // null materializes as Nullable<T>[] instead, there being no other way to carry
                        // one, which is a fact about the values and not about the type, so the type the
                        // column advertises is the non-null one
                        var component = relType.getComponentType();
                        if (component is null)
                            return null;

                        var element = context.Registry.GetClrType(component).MakeArrayType();
                        return clrType is null || clrType == element || clrType == typeof(object)
                            ? new CompositeClrTypeMapping(context, relType, element)
                            : null;
                    }

                case nameof(SqlTypeName.MAP):
                    {
                        var keyType = relType.getKeyType();
                        var valueType = relType.getValueType();
                        if (keyType is null || valueType is null)
                            return null;

                        var map = typeof(Dictionary<,>).MakeGenericType(context.Registry.GetClrType(keyType), context.Registry.GetClrType(valueType));
                        return clrType is null || clrType == map || clrType == typeof(object)
                            ? new CompositeClrTypeMapping(context, relType, map)
                            : null;
                    }
            }

            return null;
        }

    }

}
