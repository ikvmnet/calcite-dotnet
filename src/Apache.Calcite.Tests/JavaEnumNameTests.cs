using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.avatica;
using org.apache.calcite.rel.core;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Guards the invariant every dispatch on a Calcite enum depends on.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A Java enum's ordinals are an artefact of declaration order in the version compiled against. Insert a
    /// constant upstream and every ordinal after it shifts, so <c>(SqlKind.__Enum)kind.ordinal()</c> silently
    /// starts naming the wrong thing against a Calcite the code was not compiled with. Dispatch is therefore
    /// by <c>name()</c>, with <c>nameof</c> supplying the case label — a compile-time constant, so the switch
    /// is still a jump table and the compiler still checks the member exists.
    /// </para>
    /// <para>
    /// That only holds while the C# member name IKVM exposes equals the name Java reports. If IKVM ever had
    /// to mangle one, <c>nameof</c> and <c>name()</c> would disagree and the case would quietly never match —
    /// which is what these assert against, for every constant the code dispatches on.
    /// </para>
    /// </remarks>
    [TestClass]
    public class JavaEnumNameTests
    {

        [TestMethod]
        public void SqlTypeNameConstantsAreNamedAsDeclared()
        {
            Assert.AreEqual(nameof(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN.name());
            Assert.AreEqual(nameof(SqlTypeName.TINYINT), SqlTypeName.TINYINT.name());
            Assert.AreEqual(nameof(SqlTypeName.SMALLINT), SqlTypeName.SMALLINT.name());
            Assert.AreEqual(nameof(SqlTypeName.INTEGER), SqlTypeName.INTEGER.name());
            Assert.AreEqual(nameof(SqlTypeName.BIGINT), SqlTypeName.BIGINT.name());
            Assert.AreEqual(nameof(SqlTypeName.DECIMAL), SqlTypeName.DECIMAL.name());
            Assert.AreEqual(nameof(SqlTypeName.REAL), SqlTypeName.REAL.name());
            Assert.AreEqual(nameof(SqlTypeName.FLOAT), SqlTypeName.FLOAT.name());
            Assert.AreEqual(nameof(SqlTypeName.DOUBLE), SqlTypeName.DOUBLE.name());
            Assert.AreEqual(nameof(SqlTypeName.CHAR), SqlTypeName.CHAR.name());
            Assert.AreEqual(nameof(SqlTypeName.VARCHAR), SqlTypeName.VARCHAR.name());
            Assert.AreEqual(nameof(SqlTypeName.BINARY), SqlTypeName.BINARY.name());
            Assert.AreEqual(nameof(SqlTypeName.VARBINARY), SqlTypeName.VARBINARY.name());
            Assert.AreEqual(nameof(SqlTypeName.DATE), SqlTypeName.DATE.name());
            Assert.AreEqual(nameof(SqlTypeName.TIME), SqlTypeName.TIME.name());
            Assert.AreEqual(nameof(SqlTypeName.TIME_TZ), SqlTypeName.TIME_TZ.name());
            Assert.AreEqual(nameof(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE.name());
            Assert.AreEqual(nameof(SqlTypeName.TIMESTAMP), SqlTypeName.TIMESTAMP.name());
            Assert.AreEqual(nameof(SqlTypeName.TIMESTAMP_TZ), SqlTypeName.TIMESTAMP_TZ.name());
            Assert.AreEqual(nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name());
            Assert.AreEqual(nameof(SqlTypeName.OTHER), SqlTypeName.OTHER.name());
            Assert.AreEqual(nameof(SqlTypeName.ANY), SqlTypeName.ANY.name());
            Assert.AreEqual(nameof(SqlTypeName.NULL), SqlTypeName.NULL.name());
        }

        [TestMethod]
        public void SqlKindConstantsAreNamedAsDeclared()
        {
            Assert.AreEqual(nameof(SqlKind.AND), SqlKind.AND.name());
            Assert.AreEqual(nameof(SqlKind.OR), SqlKind.OR.name());
            Assert.AreEqual(nameof(SqlKind.NOT), SqlKind.NOT.name());
            Assert.AreEqual(nameof(SqlKind.CAST), SqlKind.CAST.name());
            Assert.AreEqual(nameof(SqlKind.LITERAL), SqlKind.LITERAL.name());
            Assert.AreEqual(nameof(SqlKind.INPUT_REF), SqlKind.INPUT_REF.name());
            Assert.AreEqual(nameof(SqlKind.DYNAMIC_PARAM), SqlKind.DYNAMIC_PARAM.name());
            Assert.AreEqual(nameof(SqlKind.EQUALS), SqlKind.EQUALS.name());
            Assert.AreEqual(nameof(SqlKind.NOT_EQUALS), SqlKind.NOT_EQUALS.name());
            Assert.AreEqual(nameof(SqlKind.GREATER_THAN), SqlKind.GREATER_THAN.name());
            Assert.AreEqual(nameof(SqlKind.GREATER_THAN_OR_EQUAL), SqlKind.GREATER_THAN_OR_EQUAL.name());
            Assert.AreEqual(nameof(SqlKind.LESS_THAN), SqlKind.LESS_THAN.name());
            Assert.AreEqual(nameof(SqlKind.LESS_THAN_OR_EQUAL), SqlKind.LESS_THAN_OR_EQUAL.name());
            Assert.AreEqual(nameof(SqlKind.IS_NULL), SqlKind.IS_NULL.name());
            Assert.AreEqual(nameof(SqlKind.IS_NOT_NULL), SqlKind.IS_NOT_NULL.name());
            Assert.AreEqual(nameof(SqlKind.IS_TRUE), SqlKind.IS_TRUE.name());
            Assert.AreEqual(nameof(SqlKind.IS_NOT_TRUE), SqlKind.IS_NOT_TRUE.name());
            Assert.AreEqual(nameof(SqlKind.IS_FALSE), SqlKind.IS_FALSE.name());
            Assert.AreEqual(nameof(SqlKind.IS_NOT_FALSE), SqlKind.IS_NOT_FALSE.name());
            Assert.AreEqual(nameof(SqlKind.IS_NOT_DISTINCT_FROM), SqlKind.IS_NOT_DISTINCT_FROM.name());
            Assert.AreEqual(nameof(SqlKind.UNION), SqlKind.UNION.name());
            Assert.AreEqual(nameof(SqlKind.INTERSECT), SqlKind.INTERSECT.name());
            Assert.AreEqual(nameof(SqlKind.EXCEPT), SqlKind.EXCEPT.name());
        }

        [TestMethod]
        public void StatementTypeConstantsAreNamedAsDeclared()
        {
            Assert.AreEqual(nameof(Meta.StatementType.SELECT), Meta.StatementType.SELECT.name());
            Assert.AreEqual(nameof(Meta.StatementType.CREATE), Meta.StatementType.CREATE.name());
            Assert.AreEqual(nameof(Meta.StatementType.ALTER), Meta.StatementType.ALTER.name());
            Assert.AreEqual(nameof(Meta.StatementType.DROP), Meta.StatementType.DROP.name());
            Assert.AreEqual(nameof(Meta.StatementType.OTHER_DDL), Meta.StatementType.OTHER_DDL.name());
        }

        [TestMethod]
        public void JoinAndRowFormatConstantsAreNamedAsDeclared()
        {
            Assert.AreEqual(nameof(JoinRelType.INNER), JoinRelType.INNER.name());
            Assert.AreEqual(nameof(JoinRelType.LEFT), JoinRelType.LEFT.name());
            Assert.AreEqual(nameof(JoinRelType.RIGHT), JoinRelType.RIGHT.name());
            Assert.AreEqual(nameof(JoinRelType.FULL), JoinRelType.FULL.name());

            Assert.AreEqual(nameof(JavaRowFormat.SCALAR), JavaRowFormat.SCALAR.name());
            Assert.AreEqual(nameof(JavaRowFormat.ARRAY), JavaRowFormat.ARRAY.name());
            Assert.AreEqual(nameof(JavaRowFormat.CUSTOM), JavaRowFormat.CUSTOM.name());
        }

        /// <summary>
        /// The failure this whole approach exists to prevent, stated directly: an ordinal is a position, and
        /// a position is only meaningful against one build of Calcite.
        /// </summary>
        [TestMethod]
        public void NameIsStableWhereOrdinalIsNot()
        {
            // whatever position it occupies in the Calcite actually loaded, it is still called CAST
            Assert.AreEqual("CAST", SqlKind.CAST.name());
            Assert.AreEqual(SqlKind.CAST, SqlKind.valueOf("CAST"));

            // and the round trip holds for the type names a reader dispatches on
            foreach (var t in new[] { SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR })
                Assert.AreEqual(t, SqlTypeName.get(t.name()));
        }

    }

}
