using System;
using System.Buffers.Binary;


namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Converts between <see cref="Guid"/> and the UUID types Calcite uses, without loss.
    /// </summary>
    /// <remarks>
    /// <see cref="java.util.UUID"/> holds the sixteen bytes as two <see cref="long"/> halves in the order of
    /// the canonical <c>8-4-4-4-12</c> text, which is the order <see cref="Guid"/> reads and writes them in
    /// when <c>bigEndian</c> is set, so the halves are transferred directly.
    ///
    /// <para>At run time Calcite holds a UUID as <c>org.apache.calcite.util.UuidValue</c>, which orders the
    /// value as an unsigned 128-bit number, rather than as <see cref="java.util.UUID"/>;
    /// <c>JavaTypeFactoryImpl.getJavaClass</c> answers <c>UuidValue</c> for a UUID column, so a value bound
    /// into a plan has to be one.</para>
    /// </remarks>
    internal static class JavaUuids
    {

        /// <summary>
        /// Converts a <see cref="java.util.UUID"/> to the equivalent <see cref="Guid"/>.
        /// </summary>
        /// <param name="value">The Java UUID.</param>
        /// <returns>A <see cref="Guid"/> with the same 128 bits, most significant first.</returns>
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
        /// <param name="value">The CLR GUID.</param>
        /// <returns>A <see cref="java.util.UUID"/> with the same 128 bits, most significant first.</returns>
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
        /// <param name="value">The Calcite UUID value.</param>
        /// <returns>A <see cref="Guid"/> with the same 128 bits, most significant first.</returns>
        public static Guid ToGuid(org.apache.calcite.util.UuidValue value)
        {
            Span<byte> bytes = stackalloc byte[16];
            BinaryPrimitives.WriteInt64BigEndian(bytes, value.getMostSignificantBits());
            BinaryPrimitives.WriteInt64BigEndian(bytes.Slice(8), value.getLeastSignificantBits());
            return new Guid(bytes, bigEndian: true);
        }

        /// <summary>
        /// Converts a <see cref="Guid"/> to the equivalent <c>org.apache.calcite.util.UuidValue</c>.
        /// </summary>
        /// <param name="value">The CLR GUID.</param>
        /// <returns>A <c>UuidValue</c> with the same 128 bits, most significant first.</returns>
        public static org.apache.calcite.util.UuidValue ToUuidValue(Guid value)
        {
            return new org.apache.calcite.util.UuidValue(ToUuid(value));
        }

    }

}
