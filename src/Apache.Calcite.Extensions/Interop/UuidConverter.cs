using System;
using System.Buffers.Binary;


namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Lossless binary conversion between <see cref="Guid"/> and <see cref="java.util.UUID"/>.
    /// </summary>
    /// <remarks>
    /// Both types are the same sixteen bytes in the order the canonical <c>8-4-4-4-12</c> text writes
    /// them: <see cref="java.util.UUID"/> holds them as two <see cref="long"/> halves, and
    /// <see cref="Guid"/> reads and writes them in that order under <c>bigEndian</c>. The halves are
    /// transferred directly, avoiding any string round-trip.
    /// </remarks>
    internal static class UuidConverter
    {

        /// <summary>
        /// Converts a <see cref="java.util.UUID"/> to the equivalent <see cref="Guid"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Guid ToGuid(java.util.UUID value)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value.getMostSignificantBits());
            BinaryPrimitives.WriteInt64BigEndian(bytes.Slice(8), value.getLeastSignificantBits());
            return new Guid(bytes, bigEndian: true);
        }

        /// <summary>
        /// Converts a <see cref="Guid"/> to the equivalent <see cref="java.util.UUID"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static java.util.UUID ToUuid(Guid value)
        {
            Span<byte> bytes = stackalloc byte[16];
            value.TryWriteBytes(bytes, bigEndian: true, out _);
            return new java.util.UUID(
                BinaryPrimitives.ReadInt64BigEndian(bytes),
                BinaryPrimitives.ReadInt64BigEndian(bytes.Slice(8)));
        }

    }

}
