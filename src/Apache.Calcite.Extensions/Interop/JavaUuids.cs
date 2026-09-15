using System;
using System.Buffers.Binary;


namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Lossless binary conversion between <see cref="Guid"/> and Calcite's UUID representations.
    /// </summary>
    /// <remarks>
    /// Both types are the same sixteen bytes in the order the canonical <c>8-4-4-4-12</c> text writes
    /// them: <see cref="java.util.UUID"/> holds them as two <see cref="long"/> halves, and
    /// <see cref="Guid"/> reads and writes them in that order under <c>bigEndian</c>. The halves are
    /// transferred directly, avoiding any string round-trip.
    ///
    /// <para>Calcite's <em>runtime</em> representation is <c>org.apache.calcite.util.UuidValue</c>, not
    /// <see cref="java.util.UUID"/> — CALCITE-7716 wrapped it because <c>UUID.compareTo</c> orders the
    /// two halves as signed longs where SQL orders a UUID as an unsigned 128-bit value.
    /// <c>JavaTypeFactoryImpl.getJavaClass</c> answers <c>UuidValue.class</c> for a UUID, so that is what
    /// a generated plan casts to and what a value bound into one has to be. The wrapper exposes the same
    /// two halves, so the conversion is the same four lines either way.</para>
    /// </remarks>
    internal static class JavaUuids
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

        /// <summary>
        /// Converts an <c>org.apache.calcite.util.UuidValue</c> to the equivalent <see cref="Guid"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static Guid ToGuid(org.apache.calcite.util.UuidValue value)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value.getMostSignificantBits());
            BinaryPrimitives.WriteInt64BigEndian(bytes.Slice(8), value.getLeastSignificantBits());
            return new Guid(bytes, bigEndian: true);
        }

        /// <summary>
        /// Converts a <see cref="Guid"/> to the <c>org.apache.calcite.util.UuidValue</c> Calcite holds a
        /// UUID as at run time.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static org.apache.calcite.util.UuidValue ToUuidValue(Guid value)
        {
            return new org.apache.calcite.util.UuidValue(ToUuid(value));
        }

    }

}
