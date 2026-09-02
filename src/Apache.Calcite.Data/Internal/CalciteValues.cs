using System;
using System.Collections;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts a value between the representation Calcite's runtime holds and the representation an
    /// ADO.NET caller reads and writes.
    /// </summary>
    /// <remarks>
    /// Calcite's runtime is Java's, and it is reached through IKVM, so a value arriving from a plan is a
    /// Java object: <c>java.lang.Integer</c> for an <c>INTEGER</c>, <c>java.math.BigDecimal</c> for a
    /// <c>DECIMAL</c>, <c>java.util.List</c> for an <c>ARRAY</c> or a <c>MULTISET</c>,
    /// <c>java.util.Map</c> for a <c>MAP</c>, <c>Object[]</c> for a <c>ROW</c>. None of those is a type a
    /// .NET consumer of a <c>DbDataReader</c> expects to be handed, so none of them leaves this class.
    ///
    /// <para><b>The SQL type decides where it can.</b> Calcite's stored form for the temporal types is a
    /// count — days since the epoch for <c>DATE</c>, milliseconds for <c>TIME</c> and <c>TIMESTAMP</c> —
    /// so a <c>java.lang.Integer</c> means a date only because the column says <c>DATE</c>. The
    /// <see cref="RelDataType"/> is threaded through the whole conversion for that reason, and descends
    /// into a collection's component, a map's key and value, and a row's fields: without it
    /// <c>ARRAY[DATE '2020-01-01']</c> would materialize as a number.</para>
    ///
    /// <para><b>Where it cannot, the runtime type decides.</b> That is <see cref="SqlTypeName.ANY"/>,
    /// whose runtime representation is <c>java.lang.Object</c> and whose value is therefore whatever the
    /// table, the user-defined function or the schema put there. There is nothing in the type to read, so
    /// the value's own class is what the conversion goes on. This is also what happens inside an
    /// <c>ANY</c> — a map held in an <c>ANY</c> column has no key or value type either.</para>
    ///
    /// <para><b>An element type is measured, not declared.</b> A <c>java.util.List</c> becomes an array
    /// whose element type is the one every converted element shares — <c>int[]</c> for a list of
    /// <c>java.lang.Integer</c>, <c>Nullable{Int32}[]</c> where one of them is null, <c>object[]</c> where
    /// they disagree — and a <c>java.util.Map</c> becomes a <see cref="Dictionary{TKey, TValue}"/> the
    /// same way. A caller that wants different element types names them:
    /// <see cref="TryConvertTo"/> is what <c>GetFieldValue{T}</c>
    /// reaches for.</para>
    /// </remarks>
    internal static class CalciteValues
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The number of nanoseconds in one <see cref="TimeSpan"/> tick.
        /// </summary>
        const long NanosecondsPerTick = 100;

        /// <summary>
        /// The instant a zoned <c>TIME</c> is anchored at. Calcite carries a count of milliseconds since
        /// midnight and no offset per row, so the date half is the one IKVM.Jdbc's <c>OffsetTime</c> path
        /// uses.
        /// </summary>
        static readonly DateTimeOffset TimeEpoch = new(1, 1, 1, 0, 0, 0, TimeSpan.Zero);

        /// <summary>
        /// Returns the .NET value an ADO.NET caller reads for a value Calcite's runtime produced.
        /// </summary>
        /// <param name="value">The value as the plan produced it, or <see langword="null"/>.</param>
        /// <param name="type">The type the row type gives the value, or <see langword="null"/> where there is none.</param>
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
        /// <param name="name">The SQL type's name, as <see cref="SqlTypeName.name"/> gives it.</param>
        /// <param name="value">The value as the plan produced it, or <see langword="null"/>.</param>
        /// <returns>The .NET value.</returns>
        /// <remarks>
        /// The name and not the type, because a variant carries its payload's type as one and has no
        /// <see cref="RelDataType"/> to offer — and it is the same table either way, since the names
        /// <c>RuntimeSqlTypeName</c> uses for the temporal and binary types are <see cref="SqlTypeName"/>'s
        /// own. Those are the types this exists for: Calcite stores a <c>DATE</c> as a count of days and a
        /// <c>TIMESTAMP</c> as a count of milliseconds, so an integer is one or the other only because the
        /// type says so. Everything else is decided by the value's class.
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
        /// This is the <see cref="SqlTypeName.ANY"/> case and the inside of one. The last arm hands the
        /// value back untouched, which is the only answer for a class the framework has no counterpart
        /// for — a user-defined function returning its own type reaches it. Everything Calcite's own
        /// runtime produces is named above it.
        ///
        /// <para>A value that is already a .NET one falls through the same arm, which is what a table of
        /// this runtime supplies: its rows hold a CLR <c>int</c> where the plan's row type says
        /// <c>java.lang.Integer</c>.</para>
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
                // a variant carries its payload's type with it, which is the whole of what it is for
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
        static DateTime FromLocalDateTime(java.time.LocalDateTime value)
        {
            return new DateTime(value.getYear(), value.getMonthValue(), value.getDayOfMonth(), value.getHour(), value.getMinute(), value.getSecond())
                .AddTicks(value.getNano() / NanosecondsPerTick);
        }

        /// <summary>
        /// Returns an instant and an offset as the <see cref="DateTimeOffset"/> naming the same moment.
        /// </summary>
        static DateTimeOffset FromInstant(java.time.Instant instant, java.time.ZoneOffset offset)
        {
            var span = TimeSpan.FromSeconds(offset.getTotalSeconds());
            return new DateTimeOffset(UnixEpoch.AddMilliseconds(instant.toEpochMilli()), TimeSpan.Zero).ToOffset(span);
        }

        /// <summary>
        /// Returns a row as an array of its converted fields.
        /// </summary>
        /// <remarks>
        /// A row stays <c>object[]</c> however alike its fields happen to be: a <c>ROW(1, 2)</c> is two
        /// fields rather than an array of two, and unifying its element type would say otherwise.
        /// </remarks>
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
        /// A map holding a null key becomes an array of pairs instead: no dictionary the framework ships
        /// accepts one — <see cref="Dictionary{TKey, TValue}"/> throws for a null key whatever its key
        /// type is — and dropping the entry would lose a row's contents. Calcite reaches the case, as
        /// <c>MAP[CAST(NULL AS VARCHAR), 1]</c> validates and runs.
        /// </remarks>
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
        /// A null among elements of a value type makes the type nullable rather than
        /// <see cref="object"/>, so an <c>ARRAY[1, NULL]</c> is <c>Nullable{Int32}[]</c> and still names
        /// what it holds. An empty sequence has no type to read and is <see cref="object"/>.
        /// </remarks>
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
        /// Returns the value shaped as <paramref name="target"/> where that names element types the
        /// measured conversion could not have known about.
        /// </summary>
        /// <param name="value">The value as the plan produced it.</param>
        /// <param name="type">The type the row type gives the value, or <see langword="null"/>.</param>
        /// <param name="target">The type the caller asked for.</param>
        /// <param name="result">The shaped value.</param>
        /// <returns><see langword="true"/> where the value was shaped.</returns>
        /// <remarks>
        /// <c>GetFieldValue{T}</c> is the caller. <see cref="ToClr"/> measures an element type from the
        /// values in hand, so a caller wanting an <c>IDictionary&lt;string, object&gt;</c> where the
        /// values happen to agree on <c>int</c> is asking for a shape the measurement did not produce.
        /// The element types named here are used instead — and only where the values already have them,
        /// which is <see cref="Coerce"/>'s rule.
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
                    packed.SetValue(items[i], i);

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

            if (arguments.Length == 1 && value is java.util.Collection source)
            {
                var element = arguments[0];
                var items = Read(source, type?.getComponentType(), element);

                if (definition == typeof(ISet<>) || definition == typeof(HashSet<>))
                {
                    var set = Activator.CreateInstance(typeof(HashSet<>).MakeGenericType(element))!;
                    var add = set.GetType().GetMethod(nameof(HashSet<int>.Add))!;
                    foreach (var item in items)
                        add.Invoke(set, new object?[] { item });

                    result = set;
                    return true;
                }

                if (definition == typeof(IList<>) || definition == typeof(ICollection<>) || definition == typeof(IEnumerable<>) ||
                    definition == typeof(IReadOnlyList<>) || definition == typeof(IReadOnlyCollection<>) || definition == typeof(List<>))
                {
                    var list = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(element), items.Count)!;
                    foreach (var item in items)
                        list.Add(item);

                    result = list;
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Reads a Java collection as its elements converted to <paramref name="element"/>.
        /// </summary>
        static List<object?> Read(java.util.Collection source, RelDataType? component, Type element)
        {
            var items = new List<object?>(source.size());
            for (var i = source.iterator(); i.hasNext();)
                items.Add(Coerce(ToClr(i.next(), component), element));

            return items;
        }

        /// <summary>
        /// Returns a converted element where <paramref name="target"/> holds it, and refuses it where it
        /// does not.
        /// </summary>
        /// <remarks>
        /// Naming an element type says which of the types the values already have is wanted, not that
        /// they should be converted into it. An <c>IList&lt;long&gt;</c> over an <c>INTEGER ARRAY</c> is
        /// the refusal <c>GetInt64</c> makes over an <c>INTEGER</c> column, and for the same reason;
        /// <c>IList&lt;object&gt;</c> and <c>IDictionary&lt;string, object&gt;</c> are what a caller
        /// reaches for where the element types are not all one thing.
        /// </remarks>
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
        /// Returns the value Calcite's runtime holds for a value an ADO.NET caller supplied.
        /// </summary>
        /// <param name="value">The value as the caller supplied it, or <see langword="null"/>.</param>
        /// <returns>The Java value, or <see langword="null"/> where the value is null or <see cref="DBNull"/>.</returns>
        /// <remarks>
        /// The direction that matters more, because a CLR value left loose in a plan is a second
        /// representation of something Calcite already has one for, and whatever compares the two fails.
        /// A parameter carrying a <see cref="System.Data.DbType"/> is converted from that instead — see
        /// <see cref="ParameterBinder"/> — and this is what is left: a parameter of
        /// <see cref="System.Data.DbType.Object"/>, which is what a value of a type
        /// <see cref="CalciteTypeMap.ToDbType"/> has no name for infers, and every element inside one.
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
                    return JavaUuids.ToUuid(g);
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
        static long Milliseconds(DateTime value)
        {
            return (long)(value - UnixEpoch).TotalMilliseconds;
        }

        /// <summary>
        /// Returns a dictionary as the <c>java.util.Map</c> Calcite's runtime holds a <c>MAP</c> as.
        /// </summary>
        /// <remarks>
        /// A <c>LinkedHashMap</c> because Calcite's own <c>SqlFunctions.map</c> builds one: the entries of
        /// a map come out in the order they went in, and a <c>HashMap</c> would reorder a value on its way
        /// through a parameter.
        /// </remarks>
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
        static java.util.List ToJavaList(IEnumerable source)
        {
            var list = new java.util.ArrayList();
            foreach (var item in source)
                list.add(ToJava(item));

            return list;
        }

    }

}
