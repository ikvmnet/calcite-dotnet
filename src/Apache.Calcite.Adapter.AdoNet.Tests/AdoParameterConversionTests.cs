using System;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Tests the unsigned conversions <c>AdoEnumerable.ToProviderValue</c> performs, called through the
    /// enricher rather than through a query.
    /// </summary>
    /// <remarks>
    /// <see cref="GenericProviderCorrelationTests.CorrelatingOnAColumnConvertsItsValueForTheProvider"/>
    /// covers the conversions a real backend produces. <c>UShort</c>, <c>UInteger</c> and <c>ULong</c> come
    /// only from a column a provider describes as unsigned and wider than a byte, which neither SQL Server
    /// nor SQLite has, so they are tested here by handing a value to the enricher directly.
    /// </remarks>
    public class AdoParameterConversionTests : IDisposable
    {

        SqliteFixture _sqlite = null!;
        AdoDataSource _dataSource = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public AdoParameterConversionTests()
        {
            _sqlite = new SqliteFixture();
            _dataSource = new DbDataSourceAdoDataSource(_sqlite.DataSource, new SqliteDatabaseMetadata(_sqlite.DataSource));
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Returns what a provider would be handed for <paramref name="value"/>.
        /// </summary>
        /// <remarks>
        /// The index is <see cref="AdoCorrelationDataContext.Offset"/>, which the context resolves from its own
        /// array without consulting the context it wraps, so no outer statement is needed.
        /// </remarks>
        object? Bound(object value)
        {
            var indexes = new java.util.ArrayList();
            indexes.add(java.lang.Integer.valueOf(AdoCorrelationDataContext.Offset));

            var enricher = AdoEnumerable.CreateEnricher(_dataSource, indexes, new java.util.ArrayList(),
                new AdoCorrelationDataContext(null!, [value]));

            using var connection = _dataSource.OpenConnection();
            using var command = connection.CreateCommand();
            enricher.Enrich(command);

            return command.Parameters[0].Value;
        }

        /// <summary>
        /// A <c>ULong</c> binds as a <see cref="decimal"/>, since the upper half of the <c>ulong</c> range is
        /// outside <see cref="long"/>. A joou <c>ULong</c> holds the bits of a signed long read as unsigned.
        /// </summary>
        [Theory]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("9223372036854775807")]  // long.MaxValue, the largest whose bits are non-negative as a long
        [InlineData("9223372036854775808")]  // the smallest whose bits are negative as a long
        [InlineData("18446744073709551615")] // ulong.MaxValue
        public void AULongIsBoundAsItsUnsignedValue(string literal)
        {
            Bound(org.joou.ULong.valueOf(literal)).Should().Be(decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("65535")] // ushort.MaxValue, which is why the CLR type is an int
        public void AUShortIsBoundAsAnInt(string literal)
        {
            Bound(org.joou.UShort.valueOf(literal)).Should().Be(int.Parse(literal));
        }

        [Theory]
        [InlineData("0")]
        [InlineData("4294967295")] // uint.MaxValue, which is why the CLR type is a long
        public void AUIntegerIsBoundAsALong(string literal)
        {
            Bound(org.joou.UInteger.valueOf(literal)).Should().Be(long.Parse(literal));
        }

        /// <summary>
        /// The unsigned conversion a real backend does produce: SQL Server's <c>tinyint</c> is unsigned, and its
        /// range fits <see cref="byte"/> exactly.
        /// </summary>
        [Theory]
        [InlineData("0")]
        [InlineData("255")]
        public void AUByteIsBoundAsAByte(string literal)
        {
            Bound(org.joou.UByte.valueOf(literal)).Should().Be(byte.Parse(literal));
        }

    }

}
