using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts values between the Java representations Calcite's runtime holds and the .NET values an
    /// ADO.NET caller reads and writes, recursing into collections, maps and rows.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Where a <see cref="RelDataType"/> is known it decides the conversion, and it is carried into a
    /// collection's component type, a map's key and value types and a row's field types. Calcite stores
    /// temporal values as counts (days since the epoch for <c>DATE</c>, milliseconds for <c>TIME</c> and
    /// <c>TIMESTAMP</c>), so a <c>java.lang.Integer</c> is a date only because its type says <c>DATE</c>.
    /// Where no type is known — an <see cref="SqlTypeName.ANY"/> value, or anything inside one — the value's
    /// own Java class decides.
    /// </para>
    /// <para>
    /// A <c>java.util.List</c> becomes an array whose element type is the type every converted element
    /// shares: <c>int[]</c> for a list of <c>java.lang.Integer</c>, <c>int?[]</c> where one is null,
    /// <c>object[]</c> where they differ. A <c>java.util.Map</c> becomes a
    /// <see cref="Dictionary{TKey, TValue}"/> by the same rule. <see cref="TryConvertTo"/> produces the
    /// element types a caller names instead, and is what <c>GetFieldValue&lt;T&gt;</c> uses.
    /// </para>
    /// </remarks>
    internal static class CalciteValues
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The number of nanoseconds in one <see cref="TimeSpan"/> tick.
        /// </summary>
        const long NanosecondsPerTick = 100;

        /// <summary>
        /// The date a zoned <c>TIME</c> is placed on, 0001-01-01 at offset zero. Calcite holds such a value as
        /// milliseconds since midnight with no date and no per-row offset.
        /// </summary>
        static readonly DateTimeOffset TimeEpoch = new(1, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// Returns the .NET value for a value Calcite's runtime produced.
        /// </summary>
        /// <param name="value">The value as the plan produced it, or <see langword="null"/>.</param>
        /// <param name="type">The type the row type gives the value, or <see langword="null"/> where there is
        /// none.</param>
        /// <returns>The .NET value, or <see langword="null"/> where <paramref name="value"/> is null.</returns>
        public static object? ToClr(object? value, RelDataType? type)
        {
            if (value is null)
                return null;

            if (type is null)
                return FromRuntime(value);

            // a struct is a row, and a row is its fields; the field types are what carries a nested DATE
            if (type.isStruct() && value is object[] fields)
                return FromRow(fields, type);

            var name = type.getSqlTypeName().name();

            switch (name)
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    {
                        if (value is java.util.Collection collection)
                            return FromCollection(collection, type.getComponentType());

                        break;
                    }

                case nameof(SqlTypeName.MAP):
                    {
                        if (value is java.util.Map map)
                            return FromMap(map, type.getKeyType(), type.getValueType());

                        break;
                    }
            }

            return FromScalar(name, value);
        }

        /// <summary>
        /// Returns the .NET value for a value whose SQL type is named but is not a collection.
        /// </summary>
        /// <param name="name">The SQL type's name, as <c>SqlTypeName.name()</c> gives it.</param>
        /// <param name="value">The value as the plan produced it, or <see langword="null"/>.</param>
        /// <returns>The .NET value.</returns>
        /// <remarks>
        /// Takes the type's name rather than a <see cref="RelDataType"/> because a variant names its payload's
        /// type and has no <see cref="RelDataType"/>; the names <c>RuntimeSqlTypeName</c> uses for the temporal
        /// and binary types are <see cref="SqlTypeName"/>'s. The name decides the temporal and binary types,
        /// whose stored forms are counts and byte strings; the value's class decides everything else.
        /// </remarks>
        internal static object? FromScalar(string name, object? value)
        {
            if (value is null)
                return null;

            switch (name)
            {
                case nameof(SqlTypeName.DATE):
                    {
                        return value switch
                        {
                            java.lang.Number n => UnixEpoch.AddDays(n.longValue()),
                            java.sql.Date d => UnixEpoch.AddMilliseconds(d.getTime()),
                            _ => FromRuntime(value),
                        };
                    }

                case nameof(SqlTypeName.TIME):
                    {
                        return value switch
                        {
                            java.lang.Number n => TimeSpan.FromMilliseconds(n.longValue()),
                            java.sql.Time t => TimeSpan.FromMilliseconds(t.getTime()),
                            _ => FromRuntime(value),
                        };
                    }

                case nameof(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE):
                case nameof(SqlTypeName.TIME_TZ):
                    {
                        return value switch
                        {
                            java.lang.Number n => TimeEpoch.Add(TimeSpan.FromMilliseconds(n.longValue())),
                            java.sql.Time t => TimeEpoch.Add(TimeSpan.FromMilliseconds(t.getTime())),
                            _ => FromRuntime(value),
                        };
                    }

                case nameof(SqlTypeName.TIMESTAMP):
                    {
                        return value switch
                        {
                            java.lang.Number n => UnixEpoch.AddMilliseconds(n.longValue()),
                            java.sql.Timestamp ts => UnixEpoch.AddMilliseconds(ts.getTime()),
                            _ => FromRuntime(value),
                        };
                    }

                case nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE):
                case nameof(SqlTypeName.TIMESTAMP_TZ):
                    {
                        return value switch
                        {
                            java.lang.Number n => new DateTimeOffset(UnixEpoch.AddMilliseconds(n.longValue()), TimeSpan.Zero),
                            java.sql.Timestamp ts => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                            _ => FromRuntime(value),
                        };
                    }

                case nameof(SqlTypeName.BINARY):
                case nameof(SqlTypeName.VARBINARY):
                    {
                        return value switch
                        {
                            org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
                            byte[] b => b,
                            _ => FromRuntime(value),
                        };
                    }
            }

            return FromRuntime(value);
        }

        /// <summary>
        /// Returns the .NET value for a Java value whose type says nothing about what it holds.
        /// </summary>
        /// <param name="value">The non-null value as the plan produced it.</param>
        /// <returns>The .NET value, or <paramref name="value"/> itself where nothing corresponds to it.</returns>
        /// <remarks>
        /// Used for <see cref="SqlTypeName.ANY"/> and everything inside one. A value of a class with no .NET
        /// counterpart, such as a user-defined function's own type, and a value that is already a .NET one,
        /// are returned unchanged.
        /// </remarks>
        static object? FromRuntime(object value)
        {
            return value switch
            {
                string s => s,
                java.lang.Boolean b => b.booleanValue(),
                // Java's byte is signed and IKVM's is not, so the cast is the sign, not a narrowing
                java.lang.Byte y => (sbyte)y.byteValue(),
                java.lang.Short h => h.shortValue(),
                java.lang.Integer i => i.intValue(),
                java.lang.Long l => l.longValue(),
                java.lang.Float f => f.floatValue(),
                java.lang.Double d => d.doubleValue(),
                java.lang.Character c => c.charValue(),
                java.math.BigDecimal bd => JavaDecimals.ToDecimal(bd),
                java.math.BigInteger bi => new System.Numerics.BigInteger(bi.toByteArray(), isUnsigned: false, isBigEndian: true),
                org.apache.calcite.util.UuidValue uv => JavaUuids.ToGuid(uv),
                // and a bare UUID, which is what uuid() unwraps to and what a JDBC value carries
                java.util.UUID u => JavaUuids.ToGuid(u),
                org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
                // joou carries the unsigned integers, which Calcite reads an unsigned column as
                org.joou.UByte ub => (byte)ub.byteValue(),
                org.joou.UShort us => (ushort)us.shortValue(),
                org.joou.UInteger ui => (uint)ui.intValue(),
                org.joou.ULong ul => (ulong)ul.longValue(),
                // the java.sql types before java.util.Date, which is their base class
                java.sql.Timestamp ts => UnixEpoch.AddMilliseconds(ts.getTime()),
                java.sql.Date sd => UnixEpoch.AddMilliseconds(sd.getTime()),
                java.sql.Time st => TimeSpan.FromMilliseconds(st.getTime()),
                java.util.Date ud => UnixEpoch.AddMilliseconds(ud.getTime()),
                java.time.LocalDate ld => new DateOnly(ld.getYear(), ld.getMonthValue(), ld.getDayOfMonth()),
                java.time.LocalTime lt => new TimeOnly(lt.toNanoOfDay() / NanosecondsPerTick),
                java.time.LocalDateTime ldt => FromLocalDateTime(ldt),
                java.time.Instant it => new DateTimeOffset(UnixEpoch.AddMilliseconds(it.toEpochMilli()), TimeSpan.Zero),
                java.time.OffsetDateTime odt => FromInstant(odt.toInstant(), odt.getOffset()),
                java.time.ZonedDateTime zdt => FromInstant(zdt.toInstant(), zdt.getOffset()),
                java.time.Duration du => TimeSpan.FromTicks(du.getSeconds() * TimeSpan.TicksPerSecond + du.getNano() / NanosecondsPerTick),
                // a variant carries its payload's type with it
                org.apache.calcite.runtime.variant.VariantValue variant => CalciteVariants.ToClr(variant),
                java.util.Map m => FromMap(m, null, null),
                java.util.Collection col => FromCollection(col, null),
                // an Object[] is heterogeneous by construction, so its elements convert and its shape does not
                object[] a when a.GetType() == typeof(object[]) => FromRow(a, null),
                _ => value,
            };
        }

        /// <summary>
        /// Returns a <c>java.time.LocalDateTime</c> as the <see cref="DateTime"/> holding the same fields.
        /// </summary>
        /// <param name="value">The Java local date and time.</param>
        /// <returns>A <see cref="DateTime"/> of unspecified kind, truncated to the 100-nanosecond tick.</returns>
        static DateTime FromLocalDateTime(java.time.LocalDateTime value)
        {
            return new DateTime(value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(), value.getMinute(), value.getSecond())
                .AddTicks(value.getNano() / NanosecondsPerTick);
        }

        /// <summary>
        /// Returns an instant and an offset as the <see cref="DateTimeOffset"/> naming the same moment.
        /// </summary>
        /// <param name="instant">The instant; anything below a millisecond is dropped.</param>
        /// <param name="offset">The offset the result is expressed at.</param>
        /// <returns>The same moment, expressed at <paramref name="offset"/>.</returns>
        static DateTimeOffset FromInstant(java.time.Instant instant, java.time.ZoneOffset offset)
        {
            var span = TimeSpan.FromSeconds(offset.getTotalSeconds());
            return new DateTimeOffset(UnixEpoch.AddMilliseconds(instant.toEpochMilli()), TimeSpan.Zero).ToOffset(span);
        }

        /// <summary>
        /// Returns a row as an array of its converted fields.
        /// </summary>
        /// <remarks>
        /// A row is always <c>object[]</c>, even where its fields share a type: <c>ROW(1, 2)</c> is two fields,
        /// not an array of two integers.
        /// </remarks>
        /// <param name="fields">The row's fields as Calcite holds them.</param>
        /// <param name="type">The row type, which supplies each field's type; <see langword="null"/> where
        /// unknown.</param>
        /// <returns>A new array holding each field converted, in field order.</returns>
        static object?[] FromRow(object[] fields, RelDataType? type)
        {
            var list = type is not null && type.isStruct() ? type.getFieldList() : null;
            var row = new object?[fields.Length];
            for (var i = 0; i < fields.Length; i++)
            {
                var fieldType = list is not null && i < list.size() ? ((RelDataTypeField)list.get(i)).getType() : null;
                row[i] = ToClr(fields[i], fieldType);
            }

            return row;
        }

        /// <summary>
        /// Returns a Java collection as an array of its converted elements.
        /// </summary>
        /// <param name="source">The Java collection.</param>
        /// <param name="component">The element type, or <see langword="null"/> where unknown.</param>
        /// <returns>An array of the converted elements in iteration order, typed as <see cref="Pack"/>
        /// decides.</returns>
        static Array FromCollection(java.util.Collection source, RelDataType? component)
        {
            var items = new object?[source.size()];
            var n = 0;
            for (var i = source.iterator(); i.hasNext();)
                items[n++] = ToClr(i.next(), component);

            return Pack(items);
        }

        /// <summary>
        /// Returns a Java map as a dictionary of its converted entries.
        /// </summary>
        /// <param name="source">The Java map.</param>
        /// <param name="keyType">The key type, or <see langword="null"/> where unknown.</param>
        /// <param name="valueType">The value type, or <see langword="null"/> where unknown.</param>
        /// <returns>What <see cref="PackMap"/> makes of the converted entries.</returns>
        static object FromMap(java.util.Map source, RelDataType? keyType, RelDataType? valueType)
        {
            var count = source.size();
            var keys = new object?[count];
            var values = new object?[count];

            var n = 0;
            for (var i = source.entrySet().iterator(); i.hasNext();)
            {
                var entry = (java.util.Map.Entry)i.next();
                keys[n] = ToClr(entry.getKey(), keyType);
                values[n] = ToClr(entry.getValue(), valueType);
                n++;
            }

            return PackMap(keys, values);
        }

        /// <summary>
        /// Returns converted entries as a dictionary of the types they share.
        /// </summary>
        /// <remarks>
        /// A map with a null key becomes a <see cref="KeyValuePair{TKey, TValue}"/> array instead, because
        /// <see cref="Dictionary{TKey, TValue}"/> rejects a null key and Calcite allows one
        /// (<c>MAP[CAST(NULL AS VARCHAR), 1]</c>).
        /// </remarks>
        /// <param name="keys">The converted keys.</param>
        /// <param name="values">The converted values, parallel to <paramref name="keys"/>.</param>
        /// <returns>A <see cref="Dictionary{TKey, TValue}"/>, or a <c>KeyValuePair&lt;object?,
        /// object?&gt;[]</c> where a key is null.</returns>
        internal static object PackMap(object?[] keys, object?[] values)
        {
            var count = keys.Length;

            if (Array.IndexOf(keys, null) >= 0)
            {
                var pairs = new KeyValuePair<object?, object?>[count];
                for (var i = 0; i < count; i++)
                    pairs[i] = new KeyValuePair<object?, object?>(keys[i], values[i]);

                return pairs;
            }

            var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(Unify(keys), Unify(values)), count)!;
            for (var i = 0; i < count; i++)
                dictionary[keys[i]!] = values[i];

            return dictionary;
        }

        /// <summary>
        /// Returns the converted elements as an array of the type they share.
        /// </summary>
        /// <param name="items">The converted elements.</param>
        /// <returns><paramref name="items"/> itself where the elements share no type, otherwise a new array
        /// of the shared type.</returns>
        internal static Array Pack(object?[] items)
        {
            var element = Unify(items);
            if (element == typeof(object))
                return items;

            var array = Array.CreateInstance(element, items.Length);
            for (var i = 0; i < items.Length; i++)
                array.SetValue(items[i], i);

            return array;
        }

        /// <summary>
        /// Returns the type every element has, or <see cref="object"/> where they do not agree on one.
        /// </summary>
        /// <remarks>
        /// A null among elements of a value type makes the type nullable, so <c>ARRAY[1, NULL]</c> gives
        /// <c>int?</c>. An empty or all-null sequence gives <see cref="object"/>.
        /// </remarks>
        /// <param name="items">The converted elements; any may be <see langword="null"/>.</param>
        /// <returns>The shared runtime type, its nullable form, or <see cref="object"/>.</returns>
        static Type Unify(object?[] items)
        {
            Type? common = null;
            var nulls = false;

            foreach (var item in items)
            {
                if (item is null)
                {
                    nulls = true;
                    continue;
                }

                var type = item.GetType();
                if (common is null)
                    common = type;
                else if (common != type)
                    return typeof(object);
            }

            if (common is null)
                return typeof(object);

            return nulls && common.IsValueType ? typeof(Nullable<>).MakeGenericType(common) : common;
        }

        /// <summary>
        /// Converts a collection or map to the element types <paramref name="target"/> names, where the
        /// default conversion chose different ones.
        /// </summary>
        /// <param name="value">The value as the plan produced it.</param>
        /// <param name="type">The value's Calcite type, or <see langword="null"/>.</param>
        /// <param name="target">The type asked for.</param>
        /// <param name="result">The converted value.</param>
        /// <returns><see langword="true"/> where the value was converted.</returns>
        /// <exception cref="InvalidCastException">An element is not of the named element type, or is null and
        /// the element type cannot hold null.</exception>
        /// <remarks>
        /// <para>
        /// Handles two targets: a one-dimensional array over a <c>java.util.Collection</c>, and
        /// <see cref="Dictionary{TKey, TValue}"/>, <see cref="IDictionary{TKey, TValue}"/> or
        /// <see cref="IReadOnlyDictionary{TKey, TValue}"/> over a <c>java.util.Map</c>. A list or set type is
        /// not built: the default conversion's array already satisfies <c>IList&lt;T&gt;</c>,
        /// <c>IReadOnlyList&lt;T&gt;</c>, <c>ICollection&lt;T&gt;</c> and <c>IEnumerable&lt;T&gt;</c>.
        /// </para>
        /// <para>
        /// Naming an element type selects among the types the elements already have; it does not convert
        /// them (see <see cref="Coerce"/>). A map with a null key is not converted.
        /// </para>
        /// </remarks>
        public static bool TryConvertTo(object? value, RelDataType? type, Type target, out object? result)
        {
            result = null;
            if (value is null)
                return false;

            if (target.IsArray && target.GetArrayRank() == 1 && value is java.util.Collection array)
            {
                var element = target.GetElementType()!;
                var items = Read(array, type?.getComponentType(), element);
                var packed = Array.CreateInstance(element, items.Count);
                for (var i = 0; i < items.Count; i++)
                {
                    // Array.SetValue writes default(T) for a null into an array of a value type rather than
                    // refusing it, so a null element and a zero would be the same array afterwards
                    if (items[i] is null && element.IsValueType && Nullable.GetUnderlyingType(element) is null)
                        throw new InvalidCastException($"An element is null and a '{element.Name}' does not hold one.");

                    packed.SetValue(items[i], i);
                }

                result = packed;
                return true;
            }

            if (target.IsGenericType == false)
                return false;

            var definition = target.GetGenericTypeDefinition();
            var arguments = target.GetGenericArguments();

            if (arguments.Length == 2 && value is java.util.Map map &&
                (definition == typeof(IDictionary<,>) || definition == typeof(IReadOnlyDictionary<,>) || definition == typeof(Dictionary<,>)))
            {
                var dictionary = (IDictionary)Activator.CreateInstance(typeof(Dictionary<,>).MakeGenericType(arguments), map.size())!;
                for (var i = map.entrySet().iterator(); i.hasNext();)
                {
                    var entry = (java.util.Map.Entry)i.next();
                    var key = Coerce(ToClr(entry.getKey(), type?.getKeyType()), arguments[0]);
                    if (key is null)
                        return false;

                    dictionary[key] = Coerce(ToClr(entry.getValue(), type?.getValueType()), arguments[1]);
                }

                result = dictionary;
                return true;
            }

            return false;
        }

        /// <summary>
        /// Reads a Java collection as its elements converted to <paramref name="element"/>.
        /// </summary>
        /// <param name="source">The Java collection.</param>
        /// <param name="component">The element type, or <see langword="null"/> where unknown.</param>
        /// <param name="element">The CLR element type the caller asked for.</param>
        /// <returns>The elements in iteration order, each converted and checked by <see cref="Coerce"/>.</returns>
        static List<object?> Read(java.util.Collection source, RelDataType? component, Type element)
        {
            var items = new List<object?>(source.size());
            for (var i = source.iterator(); i.hasNext();)
                items.Add(Coerce(ToClr(i.next(), component), element));

            return items;
        }

        /// <summary>
        /// Returns a converted element unchanged where it is an instance of <paramref name="target"/> (or its
        /// underlying type, for a nullable target), and throws otherwise.
        /// </summary>
        /// <exception cref="InvalidCastException">The element is not an instance of the target type.</exception>
        /// <remarks>
        /// Asking for <c>long[]</c> over an <c>INTEGER ARRAY</c> is refused, as <c>GetInt64</c> refuses an
        /// <c>INTEGER</c> column; <c>object</c> is the element type for mixed contents.
        /// </remarks>
        /// <param name="value">A converted element, or <see langword="null"/>.</param>
        /// <param name="target">The element type asked for.</param>
        /// <returns><paramref name="value"/> unchanged.</returns>
        static object? Coerce(object? value, Type target)
        {
            if (value is null)
                return null;

            var underlying = Nullable.GetUnderlyingType(target) ?? target;
            if (underlying.IsInstanceOfType(value))
                return value;

            throw new InvalidCastException($"Cannot convert value of type '{value.GetType().Name}' to '{target.Name}'.");
        }

        /// <summary>
        /// Returns the Java representation Calcite's runtime holds for a .NET value, chosen by the value's
        /// own type and applied recursively to dictionaries and sequences.
        /// </summary>
        /// <param name="value">The .NET value, or <see langword="null"/>.</param>
        /// <returns>The Java value, or <see langword="null"/> where the value is null or <see cref="DBNull"/>.
        /// A value of a type not listed is returned unchanged.</returns>
        /// <remarks>
        /// A <see cref="DateTime"/> of unspecified kind is taken as UTC. A dictionary becomes a
        /// <c>java.util.Map</c> and any other sequence except a string a <c>java.util.List</c>.
        /// </remarks>
        public static object? ToJava(object? value)
        {
            if (value is null || value is DBNull)
                return null;

            switch (value)
            {
                case string s:
                    return s;
                case bool b:
                    return java.lang.Boolean.valueOf(b);
                case sbyte sb:
                    return java.lang.Byte.valueOf(unchecked((byte)sb));
                case byte by:
                    return org.joou.UByte.valueOf(by);
                case short h:
                    return java.lang.Short.valueOf(h);
                case ushort us:
                    return org.joou.UShort.valueOf(us);
                case int i:
                    return java.lang.Integer.valueOf(i);
                case uint ui:
                    return org.joou.UInteger.valueOf(ui);
                case long l:
                    return java.lang.Long.valueOf(l);
                case ulong ul:
                    return org.joou.ULong.valueOf(unchecked((long)ul));
                case float f:
                    return java.lang.Float.valueOf(f);
                case double d:
                    return java.lang.Double.valueOf(d);
                case decimal m:
                    return JavaDecimals.ToBigDecimal(m);
                case System.Numerics.BigInteger bi:
                    return new java.math.BigInteger(bi.ToByteArray(isUnsigned: false, isBigEndian: true));
                // a CHAR is a string in Calcite's runtime, so one character is a string of one
                case char c:
                    return c.ToString();
                case Guid g:
                    return JavaUuids.ToUuidValue(g);
                case byte[] bytes:
                    return new org.apache.calcite.avatica.util.ByteString(bytes);
                case DateTime dt:
                    return java.lang.Long.valueOf(Milliseconds(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt.ToUniversalTime()));
                case DateTimeOffset dto:
                    return java.lang.Long.valueOf(Milliseconds(dto.UtcDateTime));
                case DateOnly date:
                    return java.lang.Integer.valueOf((int)(new DateTime(date.Year, date.Month, date.Day, 0, 0, 0, DateTimeKind.Utc) - UnixEpoch).TotalDays);
                case TimeOnly time:
                    return java.lang.Integer.valueOf((int)time.ToTimeSpan().TotalMilliseconds);
                case TimeSpan span:
                    return java.lang.Integer.valueOf((int)span.TotalMilliseconds);
                case IDictionary dictionary:
                    return ToJavaMap(dictionary);
                // a string is a sequence of characters and is answered above; everything else that
                // enumerates is a collection, which is what Calcite holds an ARRAY or a MULTISET as
                case IEnumerable sequence:
                    return ToJavaList(sequence);
                default:
                    return value;
            }
        }

        /// <summary>
        /// Returns the milliseconds since the epoch of a UTC <see cref="DateTime"/>.
        /// </summary>
        /// <param name="value">A UTC date and time.</param>
        /// <returns>The whole milliseconds since the epoch, truncated toward zero.</returns>
        static long Milliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Returns a dictionary as the <c>java.util.Map</c> Calcite's runtime holds a <c>MAP</c> as.
        /// </summary>
        /// <remarks>
        /// A <c>LinkedHashMap</c>, as Calcite's <c>SqlFunctions.map</c> builds, so entries keep their order.
        /// </remarks>
        /// <param name="source">The dictionary; each key and value is converted with <see cref="ToJava"/>.</param>
        /// <returns>A new <c>java.util.LinkedHashMap</c> in the dictionary's enumeration order.</returns>
        static java.util.Map ToJavaMap(IDictionary source)
        {
            var map = new java.util.LinkedHashMap();
            for (var i = source.GetEnumerator(); i.MoveNext();)
                map.put(ToJava(i.Key), ToJava(i.Value));

            return map;
        }

        /// <summary>
        /// Returns a sequence as the <c>java.util.List</c> Calcite's runtime holds an <c>ARRAY</c> or a
        /// <c>MULTISET</c> as.
        /// </summary>
        /// <param name="source">The sequence; each item is converted with <see cref="ToJava"/>.</param>
        /// <returns>A new <c>java.util.ArrayList</c> in enumeration order.</returns>
        static java.util.List ToJavaList(IEnumerable source)
        {
            var list = new java.util.ArrayList();
            foreach (var item in source)
                list.add(ToJava(item));

            return list;
        }

    }

}
