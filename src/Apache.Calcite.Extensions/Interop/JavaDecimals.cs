using System;
using System.Buffers.Binary;


namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Converts between <see cref="decimal"/> and <see cref="java.math.BigDecimal"/> through the unscaled
    /// integer, without a string round trip.
    /// </summary>
    /// <remarks>
    /// Both types are an integer mantissa and a decimal scale. <see cref="decimal"/> has a 96-bit mantissa and
    /// a scale of 0 to 28; <see cref="java.math.BigDecimal"/> has an arbitrary-precision mantissa and a signed
    /// 32-bit scale, so the conversion to <see cref="decimal"/> can round or overflow.
    /// </remarks>
    internal static class JavaDecimals
    {

        /// <summary>
        /// Converts a <see cref="decimal"/> to the <see cref="java.math.BigDecimal"/> of the same value and
        /// scale.
        /// </summary>
        public static java.math.BigDecimal ToBigDecimal(decimal value)
        {
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
            int lo = bits[0], mid = bits[1], hi = bits[2], flags = bits[3];
            var isNegative = (flags & unchecked((int)0x80000000)) != 0;
            var scale = (flags >> 16) & 0x7F;

            // BigInteger takes the magnitude as a managed big-endian byte array
            var magnitude = new byte[12];
            var span = magnitude.AsSpan();
            BinaryPrimitives.WriteInt32BigEndian(span, hi);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(4), mid);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(8), lo);

            var signum = (lo | mid | hi) == 0 ? 0 : (isNegative ? -1 : 1);
            var unscaled = new java.math.BigInteger(signum, magnitude);
            return new java.math.BigDecimal(unscaled, scale);
        }

        /// <summary>
        /// Converts a <see cref="java.math.BigDecimal"/> to a <see cref="decimal"/>.
        /// </summary>
        /// <remarks>
        /// A scale above 28 is rounded to 28, half to even, and a negative scale is brought to 0.
        /// </remarks>
        /// <exception cref="OverflowException">The unscaled value does not fit in 96 bits.</exception>
        public static decimal ToDecimal(java.math.BigDecimal value)
        {
            // decimal supports a scale of 0 to 28 only
            var scale = value.scale();
            if (scale > 28)
                value = value.setScale(28, java.math.RoundingMode.HALF_EVEN);
            else if (scale < 0)
                value = value.setScale(0);

            scale = value.scale();
            var unscaled = value.unscaledValue();
            var sign = unscaled.signum();
            if (sign == 0)
                return 0m;

            var abs = unscaled.abs();
            if (abs.bitLength() > 96)
                throw new OverflowException("BigDecimal magnitude exceeds System.Decimal range.");

            // toByteArray is big-endian two's complement and may carry a leading zero sign byte; right-align
            // it into twelve bytes
            var bytes = abs.toByteArray();
            Span<byte> mag = stackalloc byte[12];
            mag.Clear();
            var src = bytes.AsSpan();
            if (src.Length > 12)
                src = src.Slice(src.Length - 12);
            src.CopyTo(mag.Slice(12 - src.Length));

            var hi = BinaryPrimitives.ReadInt32BigEndian(mag);
            var mid = BinaryPrimitives.ReadInt32BigEndian(mag.Slice(4));
            var lo = BinaryPrimitives.ReadInt32BigEndian(mag.Slice(8));

            return new decimal(lo, mid, hi, sign < 0, (byte)scale);
        }

    }

}
