using Apache.Calcite.Extensions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.avatica;
using org.apache.calcite.rel.core;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
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
    public class JavaEnumNameTests
    {

        [Fact]
        public void SqlTypeNameConstantsAreNamedAsDeclared()
        {
            Assert.Equal(nameof(SqlTypeName.BOOLEAN), SqlTypeName.BOOLEAN.name());
            Assert.Equal(nameof(SqlTypeName.TINYINT), SqlTypeName.TINYINT.name());
            Assert.Equal(nameof(SqlTypeName.SMALLINT), SqlTypeName.SMALLINT.name());
            Assert.Equal(nameof(SqlTypeName.INTEGER), SqlTypeName.INTEGER.name());
            Assert.Equal(nameof(SqlTypeName.BIGINT), SqlTypeName.BIGINT.name());
            Assert.Equal(nameof(SqlTypeName.DECIMAL), SqlTypeName.DECIMAL.name());
            Assert.Equal(nameof(SqlTypeName.REAL), SqlTypeName.REAL.name());
            Assert.Equal(nameof(SqlTypeName.FLOAT), SqlTypeName.FLOAT.name());
            Assert.Equal(nameof(SqlTypeName.DOUBLE), SqlTypeName.DOUBLE.name());
            Assert.Equal(nameof(SqlTypeName.CHAR), SqlTypeName.CHAR.name());
            Assert.Equal(nameof(SqlTypeName.VARCHAR), SqlTypeName.VARCHAR.name());
            Assert.Equal(nameof(SqlTypeName.BINARY), SqlTypeName.BINARY.name());
            Assert.Equal(nameof(SqlTypeName.VARBINARY), SqlTypeName.VARBINARY.name());
            Assert.Equal(nameof(SqlTypeName.DATE), SqlTypeName.DATE.name());
            Assert.Equal(nameof(SqlTypeName.TIME), SqlTypeName.TIME.name());
            Assert.Equal(nameof(SqlTypeName.TIME_TZ), SqlTypeName.TIME_TZ.name());
            Assert.Equal(nameof(SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE), SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE.name());
            Assert.Equal(nameof(SqlTypeName.TIMESTAMP), SqlTypeName.TIMESTAMP.name());
            Assert.Equal(nameof(SqlTypeName.TIMESTAMP_TZ), SqlTypeName.TIMESTAMP_TZ.name());
            Assert.Equal(nameof(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE), SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name());
            Assert.Equal(nameof(SqlTypeName.OTHER), SqlTypeName.OTHER.name());
            Assert.Equal(nameof(SqlTypeName.ANY), SqlTypeName.ANY.name());
            Assert.Equal(nameof(SqlTypeName.NULL), SqlTypeName.NULL.name());
        }

        [Fact]
        public void SqlKindConstantsAreNamedAsDeclared()
        {
            Assert.Equal(nameof(SqlKind.AND), SqlKind.AND.name());
            Assert.Equal(nameof(SqlKind.OR), SqlKind.OR.name());
            Assert.Equal(nameof(SqlKind.NOT), SqlKind.NOT.name());
            Assert.Equal(nameof(SqlKind.CAST), SqlKind.CAST.name());
            Assert.Equal(nameof(SqlKind.LITERAL), SqlKind.LITERAL.name());
            Assert.Equal(nameof(SqlKind.INPUT_REF), SqlKind.INPUT_REF.name());
            Assert.Equal(nameof(SqlKind.DYNAMIC_PARAM), SqlKind.DYNAMIC_PARAM.name());
            Assert.Equal(nameof(SqlKind.EQUALS), SqlKind.EQUALS.name());
            Assert.Equal(nameof(SqlKind.NOT_EQUALS), SqlKind.NOT_EQUALS.name());
            Assert.Equal(nameof(SqlKind.GREATER_THAN), SqlKind.GREATER_THAN.name());
            Assert.Equal(nameof(SqlKind.GREATER_THAN_OR_EQUAL), SqlKind.GREATER_THAN_OR_EQUAL.name());
            Assert.Equal(nameof(SqlKind.LESS_THAN), SqlKind.LESS_THAN.name());
            Assert.Equal(nameof(SqlKind.LESS_THAN_OR_EQUAL), SqlKind.LESS_THAN_OR_EQUAL.name());
            Assert.Equal(nameof(SqlKind.IS_NULL), SqlKind.IS_NULL.name());
            Assert.Equal(nameof(SqlKind.IS_NOT_NULL), SqlKind.IS_NOT_NULL.name());
            Assert.Equal(nameof(SqlKind.IS_TRUE), SqlKind.IS_TRUE.name());
            Assert.Equal(nameof(SqlKind.IS_NOT_TRUE), SqlKind.IS_NOT_TRUE.name());
            Assert.Equal(nameof(SqlKind.IS_FALSE), SqlKind.IS_FALSE.name());
            Assert.Equal(nameof(SqlKind.IS_NOT_FALSE), SqlKind.IS_NOT_FALSE.name());
            Assert.Equal(nameof(SqlKind.IS_NOT_DISTINCT_FROM), SqlKind.IS_NOT_DISTINCT_FROM.name());
            Assert.Equal(nameof(SqlKind.UNION), SqlKind.UNION.name());
            Assert.Equal(nameof(SqlKind.INTERSECT), SqlKind.INTERSECT.name());
            Assert.Equal(nameof(SqlKind.EXCEPT), SqlKind.EXCEPT.name());
        }

        [Fact]
        public void StatementTypeConstantsAreNamedAsDeclared()
        {
            Assert.Equal(nameof(Meta.StatementType.SELECT), Meta.StatementType.SELECT.name());
            Assert.Equal(nameof(Meta.StatementType.CREATE), Meta.StatementType.CREATE.name());
            Assert.Equal(nameof(Meta.StatementType.ALTER), Meta.StatementType.ALTER.name());
            Assert.Equal(nameof(Meta.StatementType.DROP), Meta.StatementType.DROP.name());
            Assert.Equal(nameof(Meta.StatementType.OTHER_DDL), Meta.StatementType.OTHER_DDL.name());
        }

        [Fact]
        public void JoinAndRowFormatConstantsAreNamedAsDeclared()
        {
            Assert.Equal(nameof(JoinRelType.INNER), JoinRelType.INNER.name());
            Assert.Equal(nameof(JoinRelType.LEFT), JoinRelType.LEFT.name());
            Assert.Equal(nameof(JoinRelType.RIGHT), JoinRelType.RIGHT.name());
            Assert.Equal(nameof(JoinRelType.FULL), JoinRelType.FULL.name());

            Assert.Equal(nameof(JavaRowFormat.SCALAR), JavaRowFormat.SCALAR.name());
            Assert.Equal(nameof(JavaRowFormat.ARRAY), JavaRowFormat.ARRAY.name());
            Assert.Equal(nameof(JavaRowFormat.CUSTOM), JavaRowFormat.CUSTOM.name());
        }

        /// <summary>
        /// The failure this whole approach exists to prevent, stated directly: an ordinal is a position, and
        /// a position is only meaningful against one build of Calcite.
        /// </summary>
        [Fact]
        public void NameIsStableWhereOrdinalIsNot()
        {
            // whatever position it occupies in the Calcite actually loaded, it is still called CAST
            Assert.Equal("CAST", SqlKind.CAST.name());
            Assert.Equal(SqlKind.CAST, SqlKind.valueOf("CAST"));

            // and the round trip holds for the type names a reader dispatches on
            foreach (var t in new[] { SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP, SqlTypeName.VARCHAR })
                Assert.Equal(t, SqlTypeName.get(t.name()));
        }

    }

}
