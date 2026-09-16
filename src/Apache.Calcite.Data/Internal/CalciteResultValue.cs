using System;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Thin wrapper over an object returned by Calcite. Provides the final conversion methods to coerce the type to and from various CLR
    /// types.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Nothing here knows a Java class or a <see cref="SqlTypeName"/>, and every accessor is one lookup
    /// in the mapping table.</b> A typed getter answers where an entry pairs the column's
    /// <see cref="RelDataType"/> with the type that getter returns, and refuses where none does — the same
    /// rule <see cref="GetFieldValue{T}"/> follows and the same rule that decides what may be written, so
    /// what a column can be read as is one table and not a table plus a switch that drifts from it. Which
    /// classes Calcite holds a value in belongs to the mapping; a <c>DATE</c> is a count of days and a
    /// <c>TIMESTAMP</c> a count of milliseconds, and neither is therefore an integer to a caller.
    /// </para>
    ///
    /// <para>
    /// Three Calcite types say nothing about what they hold — <c>ANY</c>, <c>OTHER</c> and <c>VARIANT</c> —
    /// and there <b>the value's own class stands in for the declared type</b>. Standing in for it is all it
    /// does: a <c>java.lang.Integer</c> in an <c>ANY</c> column is an <c>INTEGER</c>, so it reads through
    /// <see cref="GetInt32"/> and <see cref="GetInt64"/> refuses it exactly as it refuses an
    /// <c>INTEGER</c> column. What it adds is the case no column type could state: a
    /// <c>java.sql.Timestamp</c> or a <c>java.time.LocalDate</c> says what it is by being what it is, and
    /// <c>ANY</c> is not <c>TIMESTAMP</c> or <c>DATE</c>. Which types those are is
    /// <see cref="ClrTypeMapping.DescribesValue"/>, so this does not carry a list of its own.
    /// </para>
    ///
    /// <para><see cref="CalciteValues"/> holds the conversion itself, in both directions and recursively,
    /// so that a collection of an <c>ANY</c> is read the same way the <c>ANY</c> is.</para>
    /// </remarks>
    internal readonly struct CalciteResultValue
    {

        readonly RelDataType _type;
        readonly ClrTypeRegistry _registry;
        readonly ClrTypeMapping? _mapping;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="registry">The mappings the value is read through.</param>
        /// <param name="mapping">The mapping the column reads back through, or <see langword="null"/> where
        /// the chain has none for its type.</param>
        /// <param name="value"></param>
        /// <remarks>
        /// The mapping is handed in rather than looked up, because it is a property of the column and not
        /// of the value: resolving it here asked the registry once per cell, and the registry's key is
        /// <c>getFullTypeString()</c>.
        /// </remarks>
        public CalciteResultValue(RelDataType type, ClrTypeRegistry registry, ClrTypeMapping? mapping, object? value)
        {
            _type = type ?? throw new ArgumentNullException(nameof(type));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
            _mapping = mapping;
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance, resolving the column's mapping.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="registry">The mappings the value is read through.</param>
        /// <param name="value"></param>
        public CalciteResultValue(RelDataType type, ClrTypeRegistry registry, object? value) :
            this(type, registry, (registry ?? throw new ArgumentNullException(nameof(registry))).GetMapping(null, type ?? throw new ArgumentNullException(nameof(type))), value)
        {

        }

        /// <summary>
        /// Gets the value exactly as Calcite's runtime produced it, with nothing converted.
        /// </summary>
        /// <remarks>
        /// The one thing on this type that is not a conversion, and the only way past the rule that no Java
        /// object reaches a caller. It exists so a caller that knows Calcite can have what Calcite has —
        /// a <c>UuidValue</c>, a JTS <c>Geometry</c>, a <c>VariantValue</c>, a <c>java.util.List</c> — rather
        /// than the .NET reading of it.
        /// </remarks>
        public object? CalciteValue => _value;

        /// <summary>
        /// Returns the exception an accessor throws where the value is not the thing asked for.
        /// </summary>
        /// <param name="target"></param>
        /// <param name="inner">The refusal the mapping layer made, where it is what decided this.</param>
        /// <returns></returns>
        InvalidCastException Cannot(string target, Exception? inner = null)
        {
            var message = $"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_type}) to '{target}'";

            return inner is null ? new InvalidCastException(message) : new InvalidCastException(message, inner);
        }

        /// <summary>
        /// Returns the value converted by its own type, which is what an accessor over a column whose type
        /// says nothing reads. Null everywhere else, so an arm written against it cannot fire for a column
        /// that does say what it holds.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <b>Which columns those are is the mapping's answer, not a list of type names kept here.</b>
        /// <c>ANY</c>, <c>OTHER</c> and <c>VARIANT</c> are the three, and the registry is where that is
        /// written down; a second list here is one that falls behind it, and an accessor reading a column
        /// the registry knows about and this does not refuses a value <c>GetValue</c> returns.
        /// </remarks>
        object? Untyped()
        {
            return _value is not null && _mapping is { DescribesValue: false } mapping ? mapping.FromCalcite(_value) : null;
        }

        /// <summary>
        /// Returns the value as the column reads it back, through the mapping the result resolved once.
        /// </summary>
        /// <returns>The .NET value, or <see langword="null"/> where the value is null.</returns>
        /// <exception cref="ClrTypeMappingException">Where nothing maps the column's type.</exception>
        /// <remarks>
        /// What <c>ClrTypeRegistry.FromCalcite</c> does, with the lookup already made: it is the column's
        /// mapping and not the value's, so asking per value asked the same question of the same type for
        /// every row.
        /// </remarks>
        object? Read()
        {
            if (_value is null || _value is DBNull)
                return null;

            if (_mapping is null)
                throw new ClrTypeMappingException($"No mapping presents {_type} as a CLR type.");

            return _mapping.FromCalcite(_value);
        }

        /// <summary>
        /// Returns the value read as <typeparamref name="T"/>, which is the whole of what a typed getter is.
        /// </summary>
        /// <typeparam name="T">The CLR type the accessor answers with.</typeparam>
        /// <param name="target">The name of that type, as the refusal spells it.</param>
        /// <returns></returns>
        /// <exception cref="InvalidCastException">Where nothing carries the column to
        /// <typeparamref name="T"/>.</exception>
        /// <remarks>
        /// <para>
        /// <b>A typed getter is the mapping table asked for one pair.</b> An entry exists for the column's
        /// Calcite type and <typeparamref name="T"/>, or the column is not that thing — which is the same
        /// rule <c>GetFieldValue{T}</c> follows and the same rule that decides what may be written. There is
        /// no second table of what an accessor accepts, because a second table is a table that drifts.
        /// </para>
        /// <para>
        /// <b>And it is the Calcite type that is asked about, never the class the value arrives in.</b>
        /// Calcite stores a <c>DATE</c> as a count of days in a <c>java.lang.Integer</c> and a
        /// <c>TIMESTAMP</c> as a count of milliseconds in a <c>java.lang.Long</c>, so matching the class
        /// would let <c>GetInt32</c> answer 18263 for 2020-01-02 out of a column this reader's own
        /// <c>GetFieldType</c> calls a <see cref="DateTime"/>. The table pairs a <c>DATE</c> with
        /// <see cref="DateTime"/> and <see cref="DateOnly"/> and with nothing else, and that is the whole
        /// of what is allowed — which is what <c>ClrTypeMapping.RepresentationType</c> exists to keep
        /// apart from <c>ClrType</c>: the class a value is held in is not the type it is.
        /// </para>
        /// </remarks>
        T Get<T>(string target)
        {
            if (_value is null)
                throw Cannot(target);

            try
            {
                if (_registry.GetMapping(typeof(T), _type) is { } mapping && mapping.FromCalcite(_value) is T named)
                    return named;

                // a column whose type says nothing leaves the value's own class to stand in for it, and the
                // accessor stays exactly as strict: a java.lang.Integer under an ANY is an INTEGER, so it
                // reads through GetInt32 and GetInt64 refuses it
                if (Untyped() is T clr)
                    return clr;
            }
            catch (ClrTypeMappingException e)
            {
                throw Cannot(target, e);
            }

            throw Cannot(target);
        }

        /// <summary>
        /// Returns <c>true</c> if the value is DBNull.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <b>Two spellings, because a variant's null is an object.</b> Everywhere else Calcite holds a SQL
        /// null as a Java null, and there is no API of Calcite's that says otherwise — <c>SqlFunctions</c>
        /// has no null predicate, and <c>NullSentinel</c> is a placeholder the metadata cache and the
        /// profiler use and never reaches a row. A <c>VARIANT</c> is the exception: a <c>VariantSqlNull</c>
        /// is a SQL null that remembers the type it was null of, and a <c>VariantNull</c> is the variant
        /// type's own null, the one a JSON <c>null</c> parses to. An ADO.NET caller has one null and all
        /// three are it.
        /// </remarks>
        public bool IsDbNull()
        {
            return _value is null || (_mapping is { } mapping && mapping.IsNull(_value));
        }

        /// <summary>
        /// Implements the GetFieldValue operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <remarks>
        /// <para>
        /// <b>This is <see cref="GetValue"/> and the two things a type argument can say that it cannot.</b>
        /// The column's own reading is tried first, so <c>GetFieldValue&lt;object&gt;()</c> answers what
        /// <see cref="GetValue"/> answers and <c>GetFieldValue&lt;int[]&gt;()</c> answers an
        /// <c>INTEGER ARRAY</c> without anything further.
        /// </para>
        /// <para>
        /// What a type argument adds is a choice. A Calcite type may have more than one reading — a
        /// <c>DATE</c> is a <see cref="DateTime"/> by default and a <see cref="DateOnly"/> when asked — and
        /// naming one selects the mapping that carries it, which is the only way to reach a reading that is
        /// nobody's default. Naming element types is the same choice one level down, and reaches a shape the
        /// conversion did not produce: an <c>object[]</c> where the column reads back as an <c>int[]</c>.
        /// </para>
        /// <para>
        /// <b>And nothing else.</b> There were twenty branches below this calling the typed getters, and a
        /// last arm that handed back the Java object when a caller named its class. The first became dead
        /// when this went through the type mappings — measured, across every pair they could answer, the two
        /// arms above them answer all of it — and the second was a second way out of the rule that no Java
        /// object reaches a caller, reached without naming a method that admits it.
        /// <c>CalciteDataReader.GetCalciteValue</c> is that, by name.
        /// </para>
        /// </remarks>
        public T GetFieldValue<T>()
        {
            // IsDbNull and not a Java null, because a variant's nulls are objects: a VariantNull reaching
            // here is a null the reader has already said IsDBNull to, and reading it as anything but one
            // would make the two accessors disagree about the same value
            if (IsDbNull())
            {
                // For value types, DBNull is not assignable; for reference types, return null.
                if (default(T) is null)
                    return default!;

                throw new InvalidCastException($"Cannot convert null/DB value to {typeof(T).Name}");
            }

            var target = typeof(T);

            try
            {
                // the value as an ADO.NET caller reads it, which is what nearly every ask is for
                if (Read() is T converted)
                    return converted;

                // a conversion the chain carries only when both types are named, which is where a caller says
                // it wants one of the readings that is nobody's default
                if (_registry.GetMapping(Nullable.GetUnderlyingType(target) ?? target, _type) is { } named && named.FromCalcite(_value) is T asked)
                    return asked;

                // a collection or a map whose element types the caller named rather than the ones the values
                // measured, which is the one shape the conversion above cannot have produced
                if (CalciteValues.TryConvertTo(_value, _type, target, out var shaped) && shaped is T reshaped)
                    return reshaped;
            }
            catch (ClrTypeMappingException e)
            {
                // a mapping that refuses is this accessor refusing, and a typed getter's refusal names the
                // value, the SQL type and the target. The mapping's own account of it is the inner one
                throw Cannot(typeof(T).Name, e);
            }

            throw Cannot(typeof(T).Name);
        }

        /// <summary>
        /// Implements the GetValue operation.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            // a variant holding a null converts to one, so the coalesce is reachable and not a formality
            return _value is null ? DBNull.Value : Read() ?? DBNull.Value;
        }

        /// <summary>
        /// Implements the GetBoolean operation.
        /// </summary>
        public bool GetBoolean() => Get<bool>("Boolean");

        /// <summary>
        /// Implements the GetString operation.
        /// </summary>
        /// <returns></returns>
        public string GetString() => Get<string>("String");

        /// <summary>
        /// Implements the GetChar operation. A <c>CHAR</c> column means a character: Calcite's
        /// runtime representation of the character family is a string, so the value converts
        /// when the SQL type is <c>CHAR</c> and the string holds exactly one character. Any
        /// other SQL type or length is not a character and does not convert.
        /// </summary>
        /// <returns></returns>
        public char GetChar() => Get<char>("Char");

        /// <summary>
        /// Implements the GetBytes operation to a destination buffer.
        /// </summary>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public long GetBytes(long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (_value is null)
                return 0;

            // a BINARY arrives as a ByteString and an ANY holding binary may be either
            var bytes = GetValue() as byte[] ?? throw Cannot("Byte[]");

            if (buffer is null)
                return bytes.LongLength;

            var available = bytes.LongLength - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <summary>
        /// Implements the GetChars operation to a destination buffer.
        /// </summary>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public long GetChars(long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var s = GetString();
            if (buffer is null)
                return s.Length;

            var available = s.Length - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            s.CopyTo((int)dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <summary>
        /// Implements the GetDateTime operation. Only valid for DATE and TIMESTAMP columns.
        /// </summary>
        /// <returns></returns>
        public DateTime GetDateTime() => Get<DateTime>("DateTime");

        /// <summary>
        /// Implements the GetDateTimeOffset operation. Only valid for zoned TIMESTAMP / TIME columns.
        /// </summary>
        /// <returns></returns>
        public DateTimeOffset GetDateTimeOffset() => Get<DateTimeOffset>("DateTimeOffset");

        /// <summary>
        /// Implements the GetTimeSpan operation. Only valid for TIME columns.
        /// </summary>
        /// <returns></returns>
        public TimeSpan GetTimeSpan() => Get<TimeSpan>("TimeSpan");

        /// <summary>
        /// Implements the GetDecimal operation.
        /// </summary>
        /// <returns></returns>
        public decimal GetDecimal() => Get<decimal>("Decimal");

        /// <summary>
        /// Implements the GetDouble operation.
        /// </summary>
        /// <returns></returns>
        public double GetDouble() => Get<double>("Double");

        /// <summary>
        /// Implements the GetFloat operation.
        /// </summary>
        /// <returns></returns>
        public float GetFloat() => Get<float>("Single");

        /// <summary>
        /// Implements the GetArray operation.
        /// </summary>
        /// <returns>The column's value as an array.</returns>
        /// <exception cref="InvalidCastException">Where the column is not a collection.</exception>
        /// <remarks>
        /// <para>
        /// A collection is a core Calcite type and ADO.NET has no accessor for one, so this is the
        /// provider's. <c>ARRAY</c> and <c>MULTISET</c> both read as an array, differing in whether the
        /// order of the elements means anything rather than in what holds them, so both answer here; a
        /// <c>MAP</c> does not, being pairs.
        /// </para>
        /// <para>
        /// Strict like every other typed getter: a column that is not a collection is refused rather than
        /// wrapped in an array of one. The exception is a column whose type says nothing — an <c>ANY</c> or
        /// a <c>VARIANT</c> — where the value's own class decides here as it does everywhere else, so a
        /// list in an <c>ANY</c> column reads through this.
        /// </para>
        /// <para>
        /// The array is of whatever the element type reads back as, so an <c>INTEGER ARRAY</c> is an
        /// <c>int[]</c> and an <c>INTEGER ARRAY ARRAY</c> an <c>int[][]</c>, and an element that may be null
        /// makes it an <c>int?[]</c>, an array having no other way to carry one.
        /// </para>
        /// </remarks>
        public Array GetArray()
        {
            if (_value is null)
                throw Cannot("Array");

            // whether the column is a collection is the mapping's answer: a CollectionClrTypeMapping is what
            // an ARRAY and a MULTISET resolve to and what nothing else resolves to, so a VARBINARY — whose
            // reading is a byte[] and which is a scalar all the same — is refused here without this needing
            // to know that byte[] is the exception
            try
            {
                if (_mapping is CollectionClrTypeMapping collection)
                    return collection.FromCalcite(_value) as Array ?? throw Cannot("Array");
            }
            catch (ClrTypeMappingException e)
            {
                throw Cannot("Array", e);
            }

            // a column whose type says nothing answers if what the value turned out to be is an array
            return Untyped() is Array clr ? clr : throw Cannot("Array");
        }

        /// <summary>
        /// Implements the GetArray operation for a caller that names the element type.
        /// </summary>
        /// <typeparam name="T">The element type.</typeparam>
        /// <returns>The column's value as an array of <typeparamref name="T"/>.</returns>
        /// <exception cref="InvalidCastException">Where the column is not a collection, or nothing carries
        /// its elements to <typeparamref name="T"/>.</exception>
        /// <remarks>
        /// <para>
        /// The same conversion as <see cref="GetArray"/>, with the element type named instead of taken from
        /// the column. Naming it <em>selects a mapping</em> rather than casting the result: a <c>DATE</c>
        /// reads back as a <see cref="DateTime"/> by default and as a <see cref="DateOnly"/> when asked,
        /// because the chain carries both, and asking is the only way to reach the second.
        /// </para>
        /// <para>
        /// Which is why this walks the collection rather than converting it and casting. The conversion
        /// answers the column's own reading, and a cast can only narrow what that produced — it cannot
        /// reach a conversion that was never run.
        /// </para>
        /// <para>
        /// The array is exactly <c>T[]</c>, so a null element in a column whose elements may be null is
        /// refused where <typeparamref name="T"/> is a value type. <c>GetArray&lt;int?&gt;</c> is how a
        /// caller says it expects one.
        /// </para>
        /// </remarks>
        public T[] GetArray<T>()
        {
            // the one thing this does not share with GetFieldValue: a collection accessor refuses a null
            // column the way every other typed getter does, where GetFieldValue answers default(T)
            if (_value is null)
                throw Cannot(typeof(T).Name + "[]");

            return GetFieldValue<T[]>();
        }

        /// <summary>
        /// Implements the GetGuid operation. Calcite's runtime representation of <c>UUID</c> is a
        /// <see cref="java.util.UUID"/>, and that is the only thing this reads: a character column
        /// holding text in canonical GUID form is a character column, and parsing it here would be
        /// <see cref="GetGuid"/> answering for a type the column does not have. <c>CAST(x AS UUID)</c> is
        /// how a caller says it means one.
        /// </summary>
        /// <returns></returns>
        public Guid GetGuid() => Get<Guid>("Guid");

        /// <summary>
        /// Implements the GetInt16 operation.
        /// </summary>
        /// <returns></returns>
        public short GetInt16() => Get<short>("Int16");

        /// <summary>
        /// Implements the GetInt32 operation.
        /// </summary>
        /// <returns></returns>
        public int GetInt32() => Get<int>("Int32");

        /// <summary>
        /// Implements the GetInt64 operation.
        /// </summary>
        /// <returns></returns>
        public long GetInt64() => Get<long>("Int64");

        /// <summary>
        /// Implements the GetByte operation. A <see cref="byte"/> is a <c>TINYINT UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.UByte</c>; a signed <c>TINYINT</c> is not one.
        /// </summary>
        public byte GetByte() => Get<byte>("Byte");

        /// <summary>
        /// Implements the GetSByte operation. An <see cref="sbyte"/> is a <c>TINYINT</c>, which Java
        /// signs and Calcite holds as a <c>java.lang.Byte</c>.
        /// </summary>
        public sbyte GetSByte() => Get<sbyte>("SByte");

        /// <summary>
        /// Implements the GetUInt16 operation. A <see cref="ushort"/> is a <c>SMALLINT UNSIGNED</c>,
        /// which Calcite's runtime holds as an <c>org.joou.UShort</c>.
        /// </summary>
        public ushort GetUInt16() => Get<ushort>("UInt16");

        /// <summary>
        /// Implements the GetUInt32 operation. A <see cref="uint"/> is an <c>INTEGER UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.UInteger</c>.
        /// </summary>
        public uint GetUInt32() => Get<uint>("UInt32");

        /// <summary>
        /// Implements the GetUInt64 operation. A <see cref="ulong"/> is a <c>BIGINT UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.ULong</c>; a <c>DECIMAL</c> wide enough to hold the
        /// same number is still a <c>DECIMAL</c>.
        /// </summary>
        public ulong GetUInt64() => Get<ulong>("UInt64");

        /// <summary>
        /// Implements the GetDateOnly operation. Only valid for DATE columns.
        /// </summary>
        /// <returns></returns>
        public DateOnly GetDateOnly() => Get<DateOnly>("DateOnly");

        /// <summary>
        /// Implements the GetTimeOnly operation. Only valid for TIME columns.
        /// </summary>
        /// <returns></returns>
        public TimeOnly GetTimeOnly() => Get<TimeOnly>("TimeOnly");

    }

}
