using System;
using System.Data;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Maps between CLR types and ADO.NET <see cref="DbType"/> values for parameters.
    /// </summary>
    /// <remarks>
    /// A parameter's <see cref="DbType"/> is inferred from its value with <see cref="ToDbType"/>, and
    /// <see cref="ParameterBinder"/> turns a <see cref="DbType"/> back into the CLR type it uses to select a
    /// mapping with <see cref="ToClrType"/>. Result columns do not go through this class.
    /// </remarks>
    internal static class CalciteTypeMap
    {

        /// <summary>
        /// Returns the <see cref="DbType"/> for a CLR type, or <see cref="DbType.Object"/> where none fits.
        /// </summary>
        /// <param name="clrType">The CLR type; a nullable value type is treated as its underlying type.</param>
        /// <returns>The <see cref="DbType"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="clrType"/> is <see langword="null"/>.</exception>
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
        /// Returns the CLR type a <see cref="DbType"/> stands for, or <see cref="object"/> where none fits.
        /// </summary>
        /// <param name="dbType">The <see cref="DbType"/>.</param>
        /// <returns>The CLR type.</returns>
        /// <remarks>
        /// <see cref="DbType.Date"/> gives <see cref="DateTime"/> and <see cref="DbType.Time"/> gives
        /// <see cref="TimeSpan"/>, so this is not the inverse of <see cref="ToDbType"/> for
        /// <see cref="DateOnly"/> and <see cref="TimeOnly"/>.
        /// </remarks>
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
