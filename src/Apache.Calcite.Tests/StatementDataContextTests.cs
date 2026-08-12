using System;

using Apache.Calcite.Extensions.Prepare;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The variables a statement reads through its <c>DataContext</c>.
    /// </summary>
    /// <remarks>
    /// <c>CalciteConnectionImpl.DataContextImpl</c> puts twelve, and this port put six of them — the four
    /// timestamps all set to the same raw UTC value, with none of the three offsets Calcite computes. So a
    /// connection with a time zone got UTC from <c>CURRENT_TIMESTAMP</c>, and <c>USER</c>,
    /// <c>SYSTEM_USER</c>, <c>LOCALE</c>, the time frame set and the three streams read as null.
    /// </remarks>
    [TestClass]
    public class StatementDataContextTests
    {

        static StatementDataContext Context(string? timeZone = null, string? locale = null)
        {
            var props = new java.util.Properties();
            if (timeZone != null)
                props.setProperty(CalciteConnectionProperty.TIME_ZONE.camelName(), timeZone);
            if (locale != null)
                props.setProperty(CalciteConnectionProperty.LOCALE.camelName(), locale);

            var config = new CalciteConnectionConfigImpl(props);
            var rootSchema = CalciteSchema.createRootSchema(true);

            return new StatementDataContext(
                rootSchema,
                new JavaTypeFactoryImpl(),
                config,
                [],
                new java.util.concurrent.atomic.AtomicBoolean(false),
                0,
                []);
        }

        static long Millis(DataContext context, DataContext.Variable variable)
        {
            return ((java.lang.Long)context.get(variable.camelName)).longValue();
        }

        /// <summary>
        /// The offset the connection's time zone implies, which the port dropped.
        /// </summary>
        [TestMethod]
        public void Should_offset_the_current_timestamp_by_the_connections_time_zone()
        {
            var context = Context("GMT+05:00");

            var utc = Millis(context, DataContext.Variable.UTC_TIMESTAMP);
            var current = Millis(context, DataContext.Variable.CURRENT_TIMESTAMP);
            var local = Millis(context, DataContext.Variable.LOCAL_TIMESTAMP);

            Assert.AreEqual(TimeSpan.FromHours(5).TotalMilliseconds, current - utc);
            Assert.AreEqual(TimeSpan.FromHours(5).TotalMilliseconds, local - utc);
        }

        /// <summary>
        /// A UTC connection is where the two agree, which is why nothing caught the missing offset.
        /// </summary>
        [TestMethod]
        public void Should_not_offset_a_utc_connection()
        {
            var context = Context("GMT");

            Assert.AreEqual(
                Millis(context, DataContext.Variable.UTC_TIMESTAMP),
                Millis(context, DataContext.Variable.CURRENT_TIMESTAMP));
        }

        /// <summary>
        /// The system timestamp follows the machine's zone rather than the connection's.
        /// </summary>
        [TestMethod]
        public void Should_offset_the_system_timestamp_by_the_default_zone()
        {
            var context = Context("GMT+05:00");

            var utc = Millis(context, DataContext.Variable.UTC_TIMESTAMP);
            var sys = Millis(context, DataContext.Variable.SYS_TIMESTAMP);

            Assert.AreEqual(java.util.TimeZone.getDefault().getOffset(utc), sys - utc);
        }

        [TestMethod]
        public void Should_answer_the_variables_a_query_can_read()
        {
            var context = Context("GMT+05:00", "en_US");

            Assert.AreEqual("sa", context.get(DataContext.Variable.USER.camelName));
            Assert.AreEqual(java.lang.System.getProperty("user.name"), context.get(DataContext.Variable.SYSTEM_USER.camelName));
            Assert.IsNotNull(context.get(DataContext.Variable.TIME_ZONE.camelName));
            Assert.IsNotNull(context.get(DataContext.Variable.TIME_FRAME_SET.camelName));
            Assert.IsNotNull(context.get(DataContext.Variable.LOCALE.camelName));
            Assert.IsNotNull(context.get(DataContext.Variable.STDOUT.camelName));
        }

        /// <summary>
        /// A name nothing put there is null, and is not mistaken for a parameter.
        /// </summary>
        [TestMethod]
        public void Should_answer_null_for_an_unknown_name()
        {
            Assert.IsNull(Context().get("nothingIsCalledThis"));
        }

    }

}
