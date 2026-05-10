using System;
using System.Data;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Centralizes mappings between Calcite SQL types, CLR types, and ADO.NET <see cref="DbType"/> values.
    /// </summary>
    internal static class CalciteTypeMap
    {

        /// <summary>
        /// Returns the <see cref="DbType"/> that best represents the supplied CLR type.
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns></returns>
        public static DbType ToDbType(Type clrType)
        {
            if (clrType is null)
                throw new ArgumentNullException(nameof(clrType));

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
        /// Returns the canonical CLR type for the supplied <see cref="DbType"/>.
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
