using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Config;

using java.util;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.sql.validate;

using Xunit;

namespace Apache.Calcite.Extensions.Config.Tests
{

    public class CalciteConnectionPropertiesTests
    {

        [Fact]
        public void TestDefaults()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            Assert.False(c.ApproximateDecimal);
            Assert.False(c.ApproximateDistinctCount);
            Assert.False(c.ApproximateTopN);
            Assert.False(c.AutoTemp);
            Assert.False(c.CaseSensitive);
            Assert.Equal(SqlConformanceEnum.DEFAULT, c.Conformance);
            Assert.Equal(NullCollation.HIGH, c.DefaultNullCollation);
            Assert.Equal("standard", c.Fun);
            Assert.Equal(Lex.ORACLE, c.Lex);
            Assert.Equal("", c.Locale);
            Assert.Null(c.QuotedCasing);
            Assert.Null(c.UnquotedCasing);
            Assert.True(c.ForceDecorrelate);
            Assert.False(c.TopDownGeneralDecorrelationEnabled);
        }

        [Fact]
        public void CanSetApproximateDecimal()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.ApproximateDecimal = true;
            Assert.True(c.ApproximateDecimal);
        }

        /// <summary>
        /// The property that chooses <c>TopDownGeneralDecorrelator</c> over <c>RelDecorrelator</c> is
        /// written under the name Calcite reads it by.
        /// </summary>
        /// <remarks>
        /// The name comes from <c>camelName()</c> rather than from a literal, because a transcribed one
        /// asserts the spelling in the tree that was read and the jar is what runs.
        /// </remarks>
        [Fact]
        public void CanSetTopDownGeneralDecorrelationEnabled()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.TopDownGeneralDecorrelationEnabled = true;
            Assert.True(c.TopDownGeneralDecorrelationEnabled);
            Assert.Equal("true", p.getProperty(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.camelName()));
        }

        [Fact]
        public void CanSetConformance()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.Conformance = SqlConformanceEnum.BIG_QUERY;
            Assert.Equal(SqlConformanceEnum.BIG_QUERY, c.Conformance);
        }

        [Fact]
        public void CanSetQuotedCasing()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.QuotedCasing = Casing.TO_LOWER;
            Assert.Equal(Casing.TO_LOWER, c.QuotedCasing);
        }

        [Fact]
        public void CanSetQuotedCasingToNull()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.QuotedCasing = null;
            Assert.Null(c.QuotedCasing);
        }

        [Fact]
        public void CanSetSchemaProperty()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.SchemaProperties["host"] = "localhost";
            Assert.Equal("localhost", c.SchemaProperties["host"]);
            Assert.Equal("localhost", p.getProperty("schema.host"));
        }

    }

}
