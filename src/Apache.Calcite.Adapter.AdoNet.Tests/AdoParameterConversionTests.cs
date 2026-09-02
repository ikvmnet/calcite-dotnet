using System;

using Apache.Calcite.Adapter.AdoNet.Metadata;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// The conversions <c>AdoEnumerable.ToProviderValue</c> performs, reached through the enricher rather
    /// than through a query.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="GenericProviderCorrelationTests.CorrelatingOnAColumnConvertsItsValueForTheProvider"/>
    /// covers the branches a real backend can produce, which is the better test where one can: it binds
    /// against a live server and a wrong conversion is a wrong answer rather than a wrong object.
    /// </para>
    /// <para>
    /// Three of them it cannot reach. <c>UShort</c>, <c>UInteger</c> and <c>ULong</c> come from a column a
    /// provider describes as unsigned, and neither SQL Server nor SQLite has one wider than a
    /// <c>tinyint</c> — so those branches had no test at all, and the widest of them was converting
    /// through text until this class was written. The enricher is public and takes the context it reads
    /// from, so a value can be handed to it directly.
    /// </para>
    /// </remarks>
    [TestClass]
    public class AdoParameterConversionTests
    {

        SqliteFixture _sqlite = null!;
        AdoDataSource _dataSource = null!;

        [TestInitialize]
        public void Setup()
        {
            _sqlite = new SqliteFixture();
            _dataSource = new DbDataSourceAdoDataSource(_sqlite.DataSource, new SqliteDatabaseMetadata(_sqlite.DataSource));
        }

        [TestCleanup]
        public void Cleanup()
        {
            _sqlite?.Dispose();
        }

        /// <summary>
        /// Returns what a provider would be handed for <paramref name="value"/>.
        /// </summary>
        /// <remarks>
        /// The index is <see cref="AdoCorrelationDataContext.Offset"/>, which the context answers from its
        /// own array without consulting the one it wraps — so there is no outer statement to arrange, and
        /// the value goes in exactly where a correlation variable or a dynamic parameter would.
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
        /// The whole of a <c>ulong</c>, whose top half is outside a <see cref="long"/> and so has to be a
        /// <see cref="decimal"/>. A joou <c>ULong</c> holds the bits of a signed long read unsigned, so the
        /// value is that reinterpretation; it was read out of the type's text and parsed back before, which
        /// is the same instinct the <c>BigDecimal</c> branch was wrong for.
        /// </summary>
        [TestMethod]
        [DataRow("0")]
        [DataRow("1")]
        [DataRow("9223372036854775807")]  // long.MaxValue, the last value whose bits are not negative
        [DataRow("9223372036854775808")]  // and the first that is
        [DataRow("18446744073709551615")] // ulong.MaxValue
        public void AULongIsBoundAsItsUnsignedValue(string literal)
        {
            Bound(org.joou.ULong.valueOf(literal)).Should().Be(decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture));
        }

        [TestMethod]
        [DataRow("0")]
        [DataRow("65535")] // ushort.MaxValue, which is why the CLR type is an int
        public void AUShortIsBoundAsAnInt(string literal)
        {
            Bound(org.joou.UShort.valueOf(literal)).Should().Be(int.Parse(literal));
        }

        [TestMethod]
        [DataRow("0")]
        [DataRow("4294967295")] // uint.MaxValue, which is why the CLR type is a long
        public void AUIntegerIsBoundAsALong(string literal)
        {
            Bound(org.joou.UInteger.valueOf(literal)).Should().Be(long.Parse(literal));
        }

        /// <summary>
        /// The branch a real backend does reach, here for the contrast: a <c>tinyint</c> is the one
        /// unsigned type SQL Server describes, and its range fits the CLR type exactly.
        /// </summary>
        [TestMethod]
        [DataRow("0")]
        [DataRow("255")]
        public void AUByteIsBoundAsAByte(string literal)
        {
            Bound(org.joou.UByte.valueOf(literal)).Should().Be(byte.Parse(literal));
        }

    }

}
