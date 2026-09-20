using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Config;

using java.util;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Extensions.Config.Tests
{

    [TestClass]
    public class CalciteConnectionPropertiesTests
    {

        [TestMethod]
        public void TestDefaults()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            Assert.AreEqual(false, c.ApproximateDecimal);
            Assert.AreEqual(false, c.ApproximateDistinctCount);
            Assert.AreEqual(false, c.ApproximateTopN);
            Assert.AreEqual(false, c.AutoTemp);
            Assert.AreEqual(false, c.CaseSensitive);
            Assert.AreEqual(SqlConformanceEnum.DEFAULT, c.Conformance);
            Assert.AreEqual(NullCollation.HIGH, c.DefaultNullCollation);
            Assert.AreEqual("standard", c.Fun);
            Assert.AreEqual(Lex.ORACLE, c.Lex);
            Assert.AreEqual("", c.Locale);
            Assert.AreEqual(null, c.QuotedCasing);
            Assert.AreEqual(null, c.UnquotedCasing);
            Assert.AreEqual(true, c.ForceDecorrelate);
            Assert.AreEqual(false, c.TopDownGeneralDecorrelationEnabled);
        }

        [TestMethod]
        public void CanSetApproximateDecimal()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.ApproximateDecimal = true;
            Assert.AreEqual(true, c.ApproximateDecimal);
        }

        /// <summary>
        /// The property that chooses <c>TopDownGeneralDecorrelator</c> over <c>RelDecorrelator</c> is
        /// written under the name Calcite reads it by.
        /// </summary>
        /// <remarks>
        /// The name comes from <c>camelName()</c> rather than from a literal, because a transcribed one
        /// asserts the spelling in the tree that was read and the jar is what runs.
        /// </remarks>
        [TestMethod]
        public void CanSetTopDownGeneralDecorrelationEnabled()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.TopDownGeneralDecorrelationEnabled = true;
            Assert.AreEqual(true, c.TopDownGeneralDecorrelationEnabled);
            Assert.AreEqual("true", p.getProperty(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED.camelName()));
        }

        [TestMethod]
        public void CanSetConformance()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.Conformance = SqlConformanceEnum.BIG_QUERY;
            Assert.AreEqual(SqlConformanceEnum.BIG_QUERY, c.Conformance);
        }

        [TestMethod]
        public void CanSetQuotedCasing()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.QuotedCasing = Casing.TO_LOWER;
            Assert.AreEqual(Casing.TO_LOWER, c.QuotedCasing);
        }

        [TestMethod]
        public void CanSetQuotedCasingToNull()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.QuotedCasing = null;
            Assert.AreEqual(null, c.QuotedCasing);
        }

        [TestMethod]
        public void CanSetSchemaProperty()
        {
            var p = new Properties();
            var c = new CalciteConnectionProperties(p);
            c.SchemaProperties["host"] = "localhost";
            Assert.AreEqual("localhost", c.SchemaProperties["host"]);
            Assert.AreEqual("localhost", p.getProperty("schema.host"));
        }

    }

}
