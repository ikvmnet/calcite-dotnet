using System;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// One cell of a result row: the value Calcite produced, with its column's type and mapping, and the
    /// conversions every reader accessor goes through.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Every accessor is a lookup in the session's <see cref="ClrTypeRegistry"/>; nothing here switches on a
    /// Java class or a <see cref="SqlTypeName"/>. A typed getter answers where the registry pairs the
    /// column's <see cref="RelDataType"/> with the type the getter returns, and throws
    /// <see cref="InvalidCastException"/> where it does not. That is the same table
    /// <see cref="GetFieldValue{T}"/> and parameter binding use, so what a column can be read as and what can
    /// be written to it cannot drift apart. Following <c>Microsoft.Data.SqlClient</c>, a typed getter is a
    /// cast to one of the column's readings and never a conversion between types.
    /// </para>
    /// <para>
    /// For the three Calcite types that do not describe their values — <c>ANY</c>, <c>OTHER</c> and
    /// <c>VARIANT</c>, identified by <see cref="ClrTypeMapping.DescribesValue"/> — the value's own class
    /// stands in for the declared type, with the same strictness: a <c>java.lang.Integer</c> in an
    /// <c>ANY</c> column reads through <see cref="GetInt32"/> and is refused by <see cref="GetInt64"/>, as
    /// in an <c>INTEGER</c> column.
    /// </para>
    /// <para>
    /// <see cref="CalciteValues"/> holds the recursive conversion of collections, so a collection of
    /// <c>ANY</c> is read the same way an <c>ANY</c> is.
    /// </para>
    /// </remarks>
    internal readonly struct CalciteResultValue
    {

        readonly RelDataType _type;
        readonly ClrTypeRegistry _registry;
        readonly ClrTypeMapping? _mapping;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance with the column's mapping already resolved.
        /// </summary>
        /// <param name="type">The column's Calcite type.</param>
        /// <param name="registry">The mappings the value is read through.</param>
        /// <param name="mapping">The mapping the column reads back through, or <see langword="null"/> where
        /// the registry has none for its type.</param>
        /// <param name="value">The value as Calcite's runtime produced it.</param>
        /// <remarks>
        /// The mapping belongs to the column, so the result resolves it once and passes it to every cell
        /// rather than each cell asking the registry.
        /// </remarks>
        public CalciteResultValue(RelDataType type, ClrTypeRegistry registry, ClrTypeMapping? mapping, object? value)
        {
            _type = type ?? throw new ArgumentNullException(nameof(type));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
            _mapping = mapping;
            _value = value;
        }

        /// <summary>
        /// Initializes a new instance, resolving the column's mapping from <paramref name="registry"/>.
        /// </summary>
        /// <param name="type">The column's Calcite type.</param>
        /// <param name="registry">The mappings the value is read through.</param>
        /// <param name="value">The value as Calcite's runtime produced it.</param>
        public CalciteResultValue(RelDataType type, ClrTypeRegistry registry, object? value) :
            this(type, registry, (registry ?? throw new ArgumentNullException(nameof(registry))).GetMapping(null, type ?? throw new ArgumentNullException(nameof(type))), value)
        {

        }

        /// <summary>
        /// Gets the value exactly as Calcite's runtime produced it, with nothing converted.
        /// </summary>
        /// <remarks>
        /// The only member that can hand a Java object to a caller, through
        /// <see cref="CalciteDataReader.GetCalciteValue"/>.
        /// </remarks>
        public object? CalciteValue => _value;

        /// <summary>
        /// Returns the exception an accessor throws where the value cannot be read as <paramref name="target"/>.
        /// </summary>
        /// <param name="target">The name of the type asked for.</param>
        /// <param name="inner">The mapping layer's refusal, where that is what decided it.</param>
        /// <returns>The exception to throw.</returns>
        /// <remarks>
        /// A null gets its own message, naming <c>IsDBNull</c>, because the general message renders the
        /// value's class and value and a null has neither.
        /// </remarks>
        InvalidCastException Cannot(string target, Exception? inner = null)
        {
            var message = _value is null or DBNull
                ? $"Cannot read a null value (SQL type: {_type}) as '{target}'. IsDBNull answers true for it."
                : $"Cannot convert value of type '{_value.GetType().Name}' with value '{_value}' (SQL type: {_type}) to '{target}'";

            return inner is null ? new InvalidCastException(message) : new InvalidCastException(message, inner);
        }

        /// <summary>
        /// Returns the value converted according to its own class where the column's type does not describe
        /// its values, and <see langword="null"/> for every other column.
        /// </summary>
        /// <returns>The converted value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Which columns do not describe their values is <see cref="ClrTypeMapping.DescribesValue"/>, so this
        /// keeps no list of type names of its own.
        /// </remarks>
        object? Untyped()
        {
            return _value is not null && _mapping is { DescribesValue: false } mapping ? mapping.FromCalcite(_value) : null;
        }

        /// <summary>
        /// Returns the value as the column reads back by default, through the column's mapping.
        /// </summary>
        /// <returns>The .NET value, or <see langword="null"/> where the value is null.</returns>
        /// <exception cref="ClrTypeMappingException">No mapping covers the column's type.</exception>
        /// <remarks>
        /// <c>ClrTypeRegistry.FromCalcite</c> with the lookup already made.
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
        /// Reads the value as <typeparamref name="T"/>; the implementation of every typed getter.
        /// </summary>
        /// <typeparam name="T">The type the getter returns.</typeparam>
        /// <param name="target">The name of that type, for the exception message.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The value is null, or the registry has no mapping between
        /// the column's type and <typeparamref name="T"/>.</exception>
        /// <remarks>
        /// The mapping is looked up by the column's Calcite type, never by the class the value is held in.
        /// Calcite holds a <c>DATE</c> as a count of days in a <c>java.lang.Integer</c>, so matching on the
        /// class would let <c>GetInt32</c> return a day count from a column <c>GetFieldType</c> reports as
        /// <see cref="DateTime"/>. <c>ClrTypeMapping.RepresentationType</c> and <c>ClrType</c> keep the
        /// holding class and the presented type apart for this reason.
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
        /// Returns whether the value is a SQL null.
        /// </summary>
        /// <returns><see langword="true"/> where the value is null.</returns>
        /// <remarks>
        /// Calcite holds a SQL null as a Java null everywhere except in a <c>VARIANT</c>, where a
        /// <c>VariantSqlNull</c> (a SQL null that remembers its type) and a <c>VariantNull</c> (the variant's
        /// own null, which a JSON <c>null</c> parses to) are objects. The column's mapping recognizes those,
        /// and all three are null to an ADO.NET caller.
        /// </remarks>
        public bool IsDbNull()
        {
            return _value is null || (_mapping is { } mapping && mapping.IsNull(_value));
        }

        /// <summary>
        /// Implements <c>GetFieldValue&lt;T&gt;</c>.
        /// </summary>
        /// <typeparam name="T">The type asked for.</typeparam>
        /// <returns>The value as <typeparamref name="T"/>, or <see langword="null"/> for a null value where
        /// <typeparamref name="T"/> can hold one.</returns>
        /// <exception cref="InvalidCastException">The value is null and <typeparamref name="T"/> is a
        /// non-nullable value type, or none of the steps below reaches <typeparamref name="T"/>.</exception>
        /// <remarks>
        /// <para>Tries, in order:</para>
        /// <list type="number">
        /// <item>The column's default reading, as <see cref="GetValue"/> returns it, so
        /// <c>GetFieldValue&lt;object&gt;</c> matches <see cref="GetValue"/> and <c>GetFieldValue&lt;int[]&gt;</c>
        /// reads an <c>INTEGER ARRAY</c>.</item>
        /// <item>The registry's mapping between the column's type and <typeparamref name="T"/>, which reaches a
        /// reading that is not the default — a <c>DATE</c> as <see cref="DateOnly"/> rather than
        /// <see cref="DateTime"/>.</item>
        /// <item>A collection or map reshaped to the element types <typeparamref name="T"/> names, such as
        /// <c>object[]</c> for a column that reads back as <c>int[]</c>.</item>
        /// </list>
        /// <para>
        /// Naming the Java class the value is held in is refused, with a message pointing to
        /// <see cref="CalciteDataReader.GetCalciteValue"/>; that method is the only route to a Java object.
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

                // a collection or a map with element types the caller named rather than the ones the elements
                // share, a shape the default conversion does not produce
                if (CalciteValues.TryConvertTo(_value, _type, target, out var shaped) && shaped is T reshaped)
                    return reshaped;
            }
            catch (ClrTypeMappingException e)
            {
                // a mapping that refuses is this accessor refusing, and a typed getter's refusal names the
                // value, the SQL type and the target. The mapping's own account of it is the inner one
                throw Cannot(typeof(T).Name, e);
            }

            // a caller naming the class the value arrives in has asked for the Java object, which is the one
            // thing this cannot answer; the accessor that does has a name and the refusal gives it
            if (_value is T && target != typeof(object))
                throw new InvalidCastException(
                    $"Cannot convert value of type '{_value.GetType().Name}' with value '{_value}' (SQL type: {_type}) to '{target.Name}'. " +
                    $"It is one already: CalciteDataReader.GetCalciteValue hands out the value Calcite holds, and a typed getter answers .NET readings only.");

            throw Cannot(typeof(T).Name);
        }

        /// <summary>
        /// Implements <c>GetValue</c>: the column's default reading, or <see cref="DBNull.Value"/> for a null.
        /// </summary>
        /// <returns>The value, or <see cref="DBNull.Value"/>.</returns>
        public object GetValue()
        {
            // a variant holding a null converts to one, so the coalesce is reachable and not a formality
            return _value is null ? DBNull.Value : Read() ?? DBNull.Value;
        }

        /// <summary>
        /// Implements <c>GetBoolean</c>.
        /// </summary>
        /// <returns>The value as a <see cref="bool"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public bool GetBoolean() => Get<bool>("Boolean");

        /// <summary>
        /// Implements <c>GetString</c>.
        /// </summary>
        /// <returns>The value as a <see cref="string"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public string GetString() => Get<string>("String");

        /// <summary>
        /// Implements <c>GetChar</c>. Calcite holds character data as a string; the registry reads it as a
        /// <see cref="char"/> only for a <c>CHAR</c> column holding exactly one character.
        /// </summary>
        /// <returns>The value as a <see cref="char"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public char GetChar() => Get<char>("Char");

        /// <summary>
        /// Implements <c>GetBytes</c>: copies bytes from a binary value into <paramref name="buffer"/>.
        /// </summary>
        /// <param name="dataOffset">The offset in the value to start copying from.</param>
        /// <param name="buffer">The destination, or <see langword="null"/> to return the value's length.</param>
        /// <param name="bufferOffset">The offset in <paramref name="buffer"/> to copy to.</param>
        /// <param name="length">The maximum number of bytes to copy.</param>
        /// <returns>The number of bytes copied, the value's length where <paramref name="buffer"/> is
        /// <see langword="null"/>, or 0 for a null value.</returns>
        /// <exception cref="InvalidCastException">The value does not read back as a <see cref="byte"/> array.</exception>
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
        /// Implements <c>GetChars</c>: copies characters from a string value into <paramref name="buffer"/>.
        /// </summary>
        /// <param name="dataOffset">The offset in the value to start copying from.</param>
        /// <param name="buffer">The destination, or <see langword="null"/> to return the value's length.</param>
        /// <param name="bufferOffset">The offset in <paramref name="buffer"/> to copy to.</param>
        /// <param name="length">The maximum number of characters to copy.</param>
        /// <returns>The number of characters copied, or the value's length where <paramref name="buffer"/> is
        /// <see langword="null"/>.</returns>
        /// <exception cref="InvalidCastException">The value is null or does not read as a string.</exception>
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
        /// Implements <c>GetDateTime</c>.
        /// </summary>
        /// <returns>The value as a <see cref="DateTime"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public DateTime GetDateTime() => Get<DateTime>("DateTime");

        /// <summary>
        /// Implements <c>GetDateTimeOffset</c>.
        /// </summary>
        /// <returns>The value as a <see cref="DateTimeOffset"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public DateTimeOffset GetDateTimeOffset() => Get<DateTimeOffset>("DateTimeOffset");

        /// <summary>
        /// Implements <c>GetTimeSpan</c>.
        /// </summary>
        /// <returns>The value as a <see cref="TimeSpan"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public TimeSpan GetTimeSpan() => Get<TimeSpan>("TimeSpan");

        /// <summary>
        /// Implements <c>GetDecimal</c>.
        /// </summary>
        /// <returns>The value as a <see cref="decimal"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public decimal GetDecimal() => Get<decimal>("Decimal");

        /// <summary>
        /// Implements <c>GetDouble</c>.
        /// </summary>
        /// <returns>The value as a <see cref="double"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public double GetDouble() => Get<double>("Double");

        /// <summary>
        /// Implements <c>GetFloat</c>.
        /// </summary>
        /// <returns>The value as a <see cref="float"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public float GetFloat() => Get<float>("Single");

        /// <summary>
        /// Implements <c>GetArray</c>: reads an <c>ARRAY</c> or <c>MULTISET</c> column as an array of the
        /// element type's default reading.
        /// </summary>
        /// <returns>The column's value as an array.</returns>
        /// <exception cref="InvalidCastException">The value is null, or the column is not a collection.</exception>
        /// <remarks>
        /// A column counts as a collection where its mapping is a <see cref="CollectionClrTypeMapping"/>, so a
        /// <c>VARBINARY</c>, whose reading is a <c>byte[]</c>, is refused. A <c>MAP</c> is refused. A column
        /// that does not describe its values (<c>ANY</c>, <c>VARIANT</c>) answers where its value reads back
        /// as an array. A nullable element type gives an array of <see cref="Nullable{T}"/>.
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
        /// Implements <c>GetArray&lt;T&gt;</c>: reads a collection column as an array of
        /// <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The element type.</typeparam>
        /// <returns>The column's value as an array of <typeparamref name="T"/>.</returns>
        /// <exception cref="InvalidCastException">The value is null, the column is not a collection, or no
        /// mapping carries its elements to <typeparamref name="T"/>.</exception>
        /// <remarks>
        /// Goes through <see cref="GetFieldValue{T}"/> for <c>T[]</c>, so naming the element type selects the
        /// elements' mapping rather than casting the default result: a <c>DATE ARRAY</c> read as
        /// <see cref="DateOnly"/> converts each element. Unlike <see cref="GetFieldValue{T}"/>, a null column
        /// is refused rather than returned as <see langword="null"/>. A null element is refused where
        /// <typeparamref name="T"/> is a non-nullable value type.
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
        /// Implements <c>GetGuid</c>. Reads a <c>UUID</c> column, which Calcite holds as a <c>UuidValue</c>.
        /// A character column holding GUID text is not read as one; <c>CAST(x AS UUID)</c> makes it one.
        /// </summary>
        /// <returns>The value as a <see cref="Guid"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public Guid GetGuid() => Get<Guid>("Guid");

        /// <summary>
        /// Implements <c>GetInt16</c>.
        /// </summary>
        /// <returns>The value as a <see cref="short"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public short GetInt16() => Get<short>("Int16");

        /// <summary>
        /// Implements <c>GetInt32</c>.
        /// </summary>
        /// <returns>The value as an <see cref="int"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public int GetInt32() => Get<int>("Int32");

        /// <summary>
        /// Implements <c>GetInt64</c>.
        /// </summary>
        /// <returns>The value as a <see cref="long"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public long GetInt64() => Get<long>("Int64");

        /// <summary>
        /// Implements <c>GetByte</c>. A <see cref="byte"/> is a <c>TINYINT UNSIGNED</c>, which Calcite holds as
        /// an <c>org.joou.UByte</c>; a signed <c>TINYINT</c> is not one.
        /// </summary>
        /// <returns>The value as a <see cref="byte"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public byte GetByte() => Get<byte>("Byte");

        /// <summary>
        /// Implements <c>GetSByte</c>. An <see cref="sbyte"/> is a <c>TINYINT</c>, which Calcite holds as a
        /// signed <c>java.lang.Byte</c>.
        /// </summary>
        /// <returns>The value as an <see cref="sbyte"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public sbyte GetSByte() => Get<sbyte>("SByte");

        /// <summary>
        /// Implements <c>GetUInt16</c>. A <see cref="ushort"/> is a <c>SMALLINT UNSIGNED</c>, which Calcite
        /// holds as an <c>org.joou.UShort</c>.
        /// </summary>
        /// <returns>The value as a <see cref="ushort"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public ushort GetUInt16() => Get<ushort>("UInt16");

        /// <summary>
        /// Implements <c>GetUInt32</c>. A <see cref="uint"/> is an <c>INTEGER UNSIGNED</c>, which Calcite
        /// holds as an <c>org.joou.UInteger</c>.
        /// </summary>
        /// <returns>The value as a <see cref="uint"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public uint GetUInt32() => Get<uint>("UInt32");

        /// <summary>
        /// Implements <c>GetUInt64</c>. A <see cref="ulong"/> is a <c>BIGINT UNSIGNED</c>, which Calcite holds
        /// as an <c>org.joou.ULong</c>; a <c>DECIMAL</c> is not read as one whatever its value.
        /// </summary>
        /// <returns>The value as a <see cref="ulong"/>, read through the registry's mapping for the column's
        /// type.</returns>
        public ulong GetUInt64() => Get<ulong>("UInt64");

        /// <summary>
        /// Implements <c>GetDateOnly</c>.
        /// </summary>
        /// <returns>The value as a <see cref="DateOnly"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public DateOnly GetDateOnly() => Get<DateOnly>("DateOnly");

        /// <summary>
        /// Implements <c>GetTimeOnly</c>.
        /// </summary>
        /// <returns>The value as a <see cref="TimeOnly"/>, read through the registry's mapping for the
        /// column's type.</returns>
        public TimeOnly GetTimeOnly() => Get<TimeOnly>("TimeOnly");

    }

}
