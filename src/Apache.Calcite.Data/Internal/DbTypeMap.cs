using System;
using System.Data;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// What a <see cref="DbType"/> names, in Calcite's terms and in .NET's.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Naming only. <see cref="DbType"/> is ADO.NET's thirty-value vocabulary for talking about a type and
    /// it converts nothing: a caller who writes <see cref="DbType.Date"/> has said which Calcite type they
    /// mean, and the mapping for that type is what carries the value. Npgsql separates the two for the same
    /// reason, an <c>IDbTypeResolver</c> answering names and the converter chain answering values, and it
    /// is worth keeping apart here because the alternative is what this replaces — a
    /// <see cref="DbType"/>-keyed conversion table in the ADO.NET surface and a second one in the adapter,
    /// which disagreed about <see cref="DbType.Byte"/> until both were read side by side.
    /// </para>
    /// <para>
    /// Which Calcite type a <see cref="DbType"/> names is not here and is nobody's question: the adapter
    /// answers it in <c>AdoSchema.SqlType</c>, a port of <c>JdbcSchema.sqlType</c>, and a parameter is
    /// carried across as the type the validator inferred for its placeholder rather than as one built from
    /// a name the caller supplied. What is left is which CLR type the caller meant, which is what the type
    /// mapping is then asked about.
    /// </para>
    /// </remarks>
    internal static class DbTypeMap
    {

        /// <summary>
        /// Returns the <see cref="DbType"/> that names a CLR type, or <see cref="DbType.Object"/> where
        /// none does.
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns></returns>
        public static DbType ToDbType(Type clrType)
        {
            ArgumentNullException.ThrowIfNull(clrType);

            var t = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (t == typeof(bool)) return DbType.Boolean;
            if (t == typeof(byte)) return DbType.Byte;
            if (t == typeof(sbyte)) return DbType.SByte;
            if (t == typeof(short)) return DbType.Int16;
            if (t == typeof(ushort)) return DbType.UInt16;
            if (t == typeof(int)) return DbType.Int32;
            if (t == typeof(uint)) return DbType.UInt32;
            if (t == typeof(long)) return DbType.Int64;
            if (t == typeof(ulong)) return DbType.UInt64;
            if (t == typeof(float)) return DbType.Single;
            if (t == typeof(double)) return DbType.Double;
            if (t == typeof(decimal)) return DbType.Decimal;
            if (t == typeof(string)) return DbType.String;
            if (t == typeof(char)) return DbType.StringFixedLength;
            if (t == typeof(Guid)) return DbType.Guid;
            if (t == typeof(DateTime)) return DbType.DateTime;
            if (t == typeof(DateTimeOffset)) return DbType.DateTimeOffset;
            if (t == typeof(TimeSpan)) return DbType.Time;
            if (t == typeof(DateOnly)) return DbType.Date;
            if (t == typeof(TimeOnly)) return DbType.Time;
            if (t == typeof(byte[])) return DbType.Binary;

            return DbType.Object;
        }

        /// <summary>
        /// Returns the CLR type a <see cref="DbType"/> names.
        /// </summary>
        /// <param name="dbType"></param>
        /// <returns></returns>
        public static Type ToClrType(DbType dbType)
        {
            return dbType switch
            {
                DbType.Boolean => typeof(bool),
                DbType.Byte => typeof(byte),
                DbType.SByte => typeof(sbyte),
                DbType.Int16 => typeof(short),
                DbType.UInt16 => typeof(ushort),
                DbType.Int32 => typeof(int),
                DbType.UInt32 => typeof(uint),
                DbType.Int64 => typeof(long),
                DbType.UInt64 => typeof(ulong),
                DbType.Single => typeof(float),
                DbType.Double => typeof(double),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric => typeof(decimal),
                DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength => typeof(string),
                DbType.Guid => typeof(Guid),
                DbType.Date or DbType.DateTime or DbType.DateTime2 => typeof(DateTime),
                DbType.DateTimeOffset => typeof(DateTimeOffset),
                DbType.Time => typeof(TimeSpan),
                DbType.Binary => typeof(byte[]),
                _ => typeof(object),
            };
        }

    }

}
