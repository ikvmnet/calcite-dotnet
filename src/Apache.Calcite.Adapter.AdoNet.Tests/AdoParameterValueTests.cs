using System;
using System.Data.Common;

using Microsoft.Data.Sqlite;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the value a correlation variable becomes on its way into a provider's parameter.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A value leaves the plan as a boxed Java type, which <c>AdoEnumerable.ToProviderValue</c> unwraps to a
    /// .NET value. <see cref="GenericProviderCorrelationTests"/> checks query results against a real server;
    /// these read the bound value itself, which catches a wrong representation a comparison would not
    /// notice, and need no server.
    /// </para>
    /// <para>
    /// A correlation variable is numbered from <see cref="AdoCorrelationDataContext.Offset"/>, so the context
    /// resolves it from its own array and never consults the context it wraps.
    /// </para>
    /// </remarks>
    public class AdoParameterValueTests
    {

        sealed class Source : DbDataSource
        {

            public override string ConnectionString => "Data Source=:memory:";

            protected override DbConnection CreateDbConnection() => new SqliteConnection(ConnectionString);

        }

        /// <summary>
        /// Returns the value the given plan value is bound as, under the given SQL type.
        /// </summary>
        /// <param name="value">The plan value to bind, as the correlation data context would hold it.</param>
        /// <param name="typeName">The Calcite type the enricher is told the parameter has.</param>
        /// <returns>The value placed on the command's first parameter.</returns>
        static object? Bound(object? value, SqlTypeName typeName)
        {
            var source = new Source();
            var dataSource = new DbDataSourceAdoDataSource(source, Metadata.AdoDatabaseMetadataFactoryImpl.Instance.Create(source));

            var indexes = new java.util.ArrayList();
            indexes.add(java.lang.Integer.valueOf(AdoCorrelationDataContext.Offset));

            var typeNames = new java.util.ArrayList();
            typeNames.add(typeName.name());

            using var command = new SqliteCommand();
            AdoEnumerable.CreateEnricher(dataSource, indexes, typeNames, new AdoCorrelationDataContext(null!, [value!])).Enrich(command);

            return command.Parameters[0].Value;
        }

        /// <summary>
        /// Java's <c>byte</c> is signed and IKVM's is not, so <c>byteValue()</c> returns the two's complement
        /// bits as an unsigned CLR <see cref="byte"/>, and -56 would reach the provider as 200.
        /// </summary>
        [Fact]
        public void ANegativeTinyIntKeepsItsSign()
        {
            Assert.Equal((short)-56, Bound(java.lang.Byte.valueOf(unchecked((byte)-56)), SqlTypeName.TINYINT));
        }

        /// <summary>
        /// A <c>TINYINT</c> is bound as a <see cref="short"/> rather than as the <see cref="sbyte"/> its
        /// range would suggest, because SqlClient rejects an <see cref="sbyte"/> parameter ("The parameter data
        /// type of SByte is invalid").
        /// </summary>
        [Fact]
        public void ATinyIntIsBoundAsAShort()
        {
            Assert.IsAssignableFrom<short>(Bound(java.lang.Byte.valueOf((byte)7), SqlTypeName.TINYINT));
        }

        /// <summary>
        /// An unsigned tinyint travels as a joou value and binds as its unsigned value: 200, not -56.
        /// </summary>
        [Fact]
        public void AUTinyIntStaysUnsigned()
        {
            Assert.Equal((byte)200, Bound(org.joou.UByte.valueOf(200), SqlTypeName.UTINYINT));
        }

        [Fact]
        public void ANegativeSmallIntKeepsItsSign()
        {
            Assert.Equal((short)-300, Bound(java.lang.Short.valueOf((short)-300), SqlTypeName.SMALLINT));
        }

        [Fact]
        public void ANegativeIntegerKeepsItsSign()
        {
            Assert.Equal(-70000, Bound(java.lang.Integer.valueOf(-70000), SqlTypeName.INTEGER));
        }

        [Fact]
        public void ANegativeBigIntKeepsItsSign()
        {
            Assert.Equal(-9000000000L, Bound(java.lang.Long.valueOf(-9000000000L), SqlTypeName.BIGINT));
        }

        /// <summary>
        /// A null is bound as <see cref="DBNull"/> rather than skipped, so parameters matched by position stay
        /// aligned.
        /// </summary>
        [Fact]
        public void ANullIsBoundAsDbNull()
        {
            Assert.Equal(DBNull.Value, Bound(null, SqlTypeName.TINYINT));
        }

    }

}
