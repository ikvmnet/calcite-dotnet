using System;
using System.Buffers.Binary;


namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Lossless binary conversion between <see cref="decimal"/> and <see cref="java.math.BigDecimal"/>.
    /// </summary>
    /// <remarks>
    /// Both types store an integer mantissa plus a non-negative scale (number of decimal digits to the
    /// right of the point). <see cref="decimal"/> uses a 96-bit unsigned mantissa with scale 0..28;
    /// <see cref="java.math.BigDecimal"/> uses an arbitrary-precision <see cref="java.math.BigInteger"/>
    /// mantissa with a signed 32-bit scale. This class transfers the mantissa as raw bytes through
    /// <see cref="java.math.BigInteger"/>'s two's-complement byte representation, avoiding any
    /// string round-trip.
    /// </remarks>
    internal static class JavaDecimals
    {

        public static java.math.BigDecimal ToBigDecimal(decimal value)
        {
            // Read the four-int decimal layout directly into a stack buffer.
            Span<int> bits = stackalloc int[4];
            decimal.GetBits(value, bits);
            int lo = bits[0], mid = bits[1], hi = bits[2], flags = bits[3];
            var isNegative = (flags & unchecked((int)0x80000000)) != 0;
            var scale = (flags >> 16) & 0x7F;

            // BigInteger requires a managed byte[] for the magnitude; pack it big-endian in one pass.
            var magnitude = new byte[12];
            var span = magnitude.AsSpan();
            BinaryPrimitives.WriteInt32BigEndian(span, hi);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(4), mid);
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(8), lo);

            var signum = (lo | mid | hi) == 0 ? 0 : (isNegative ? -1 : 1);
            var unscaled = new java.math.BigInteger(signum, magnitude);
            return new java.math.BigDecimal(unscaled, scale);
        }

        public static decimal ToDecimal(java.math.BigDecimal value)
        {
            // System.Decimal requires scale in [0, 28]; normalize first.
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

            // BigInteger.toByteArray() is signed big-endian two's complement; for the absolute value
            // it may include a leading zero byte. Right-align the bytes into a 12-byte stack buffer.
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
