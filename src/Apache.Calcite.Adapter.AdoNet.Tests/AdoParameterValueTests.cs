using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.sql.type;

using System;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the value a correlation variable becomes on its way into a provider's parameter.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A value leaves the plan in Calcite's representation, which is a boxed Java type, and no ADO.NET
    /// provider knows what one of those is; <c>AdoEnumerable.ToProviderValue</c> unwraps each to the .NET
    /// value it stands for. <see cref="GenericProviderCorrelationTests"/> proves the comparisons those
    /// values take part in answer correctly against a real server, which is the stronger claim. This reads
    /// the bound value itself, which is where a representation that a comparison happens not to notice
    /// shows up — and it needs no server.
    /// </para>
    /// <para>
    /// The enricher is reached the way the generated code reaches it: a correlation variable is a parameter
    /// numbered from <see cref="AdoCorrelationDataContext.Offset"/>, so the context resolves it from its
    /// own array and never consults the context it wraps.
    /// </para>
    /// </remarks>
    [TestClass]
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
        /// <param name="value"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
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
        /// The case the sign is lost in: Java's <c>byte</c> is signed and IKVM's is not, so
        /// <c>byteValue()</c> answers the two's complement bits as an unsigned CLR <see cref="byte"/> and
        /// -56 would reach the provider as 200.
        /// </summary>
        [TestMethod]
        public void ANegativeTinyIntKeepsItsSign()
        {
            Assert.AreEqual((short)-56, Bound(java.lang.Byte.valueOf(unchecked((byte)-56)), SqlTypeName.TINYINT));
        }

        /// <summary>
        /// A <c>TINYINT</c> is bound as a <see cref="short"/> rather than as the <see cref="sbyte"/> its
        /// range would suggest: SqlClient refuses that type outright — "The parameter data type of SByte is
        /// invalid" — which is the same wall the unsigned types run into, and a <see cref="short"/> holds
        /// the whole of a signed byte's range exactly.
        /// </summary>
        [TestMethod]
        public void ATinyIntIsBoundAsAShort()
        {
            Assert.IsInstanceOfType<short>(Bound(java.lang.Byte.valueOf((byte)7), SqlTypeName.TINYINT));
        }

        /// <summary>
        /// The unsigned tinyint every real backend actually reports, which travels as a joou value and is
        /// the arm a signed one must not be confused with: 200 is 200 and not -56.
        /// </summary>
        [TestMethod]
        public void AUTinyIntStaysUnsigned()
        {
            Assert.AreEqual((byte)200, Bound(org.joou.UByte.valueOf(200), SqlTypeName.UTINYINT));
        }

        [TestMethod]
        public void ANegativeSmallIntKeepsItsSign()
        {
            Assert.AreEqual((short)-300, Bound(java.lang.Short.valueOf((short)-300), SqlTypeName.SMALLINT));
        }

        [TestMethod]
        public void ANegativeIntegerKeepsItsSign()
        {
            Assert.AreEqual(-70000, Bound(java.lang.Integer.valueOf(-70000), SqlTypeName.INTEGER));
        }

        [TestMethod]
        public void ANegativeBigIntKeepsItsSign()
        {
            Assert.AreEqual(-9000000000L, Bound(java.lang.Long.valueOf(-9000000000L), SqlTypeName.BIGINT));
        }

        /// <summary>
        /// A null is bound rather than skipped, which is what a driver matching parameters by position
        /// requires.
        /// </summary>
        [TestMethod]
        public void ANullIsBoundAsDbNull()
        {
            Assert.AreEqual(DBNull.Value, Bound(null, SqlTypeName.TINYINT));
        }

    }

}
