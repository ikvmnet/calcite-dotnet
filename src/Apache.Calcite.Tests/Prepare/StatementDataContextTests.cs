using System;

using Apache.Calcite.Extensions.Prepare;

using FluentAssertions;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Extensions.Prepare.Tests
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
    public class StatementDataContextTests
    {

        static StatementDataContext Context(string? timeZone = null, string? locale = null, System.Threading.CancellationToken cancellationToken = default)
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
                cancellationToken,
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
        [Fact]
        public void Should_offset_the_current_timestamp_by_the_connections_time_zone()
        {
            var context = Context("GMT+05:00");

            var utc = Millis(context, DataContext.Variable.UTC_TIMESTAMP);
            var current = Millis(context, DataContext.Variable.CURRENT_TIMESTAMP);
            var local = Millis(context, DataContext.Variable.LOCAL_TIMESTAMP);

            Assert.Equal(TimeSpan.FromHours(5).TotalMilliseconds, current - utc);
            Assert.Equal(TimeSpan.FromHours(5).TotalMilliseconds, local - utc);
        }

        /// <summary>
        /// A UTC connection is where the two agree, which is why nothing caught the missing offset.
        /// </summary>
        [Fact]
        public void Should_not_offset_a_utc_connection()
        {
            var context = Context("GMT");

            Assert.Equal(
                Millis(context, DataContext.Variable.UTC_TIMESTAMP),
                Millis(context, DataContext.Variable.CURRENT_TIMESTAMP));
        }

        /// <summary>
        /// The system timestamp follows the machine's zone rather than the connection's.
        /// </summary>
        [Fact]
        public void Should_offset_the_system_timestamp_by_the_default_zone()
        {
            var context = Context("GMT+05:00");

            var utc = Millis(context, DataContext.Variable.UTC_TIMESTAMP);
            var sys = Millis(context, DataContext.Variable.SYS_TIMESTAMP);

            Assert.Equal(java.util.TimeZone.getDefault().getOffset(utc), sys - utc);
        }

        [Fact]
        public void Should_answer_the_variables_a_query_can_read()
        {
            var context = Context("GMT+05:00", "en_US");

            Assert.Equal("sa", context.get(DataContext.Variable.USER.camelName));
            Assert.Equal(java.lang.System.getProperty("user.name"), context.get(DataContext.Variable.SYSTEM_USER.camelName));
            Assert.NotNull(context.get(DataContext.Variable.TIME_ZONE.camelName));
            Assert.NotNull(context.get(DataContext.Variable.TIME_FRAME_SET.camelName));
            Assert.NotNull(context.get(DataContext.Variable.LOCALE.camelName));
            Assert.NotNull(context.get(DataContext.Variable.STDOUT.camelName));
        }

        /// <summary>
        /// A name nothing put there is null, and is not mistaken for a parameter.
        /// </summary>
        [Fact]
        public void Should_answer_null_for_an_unknown_name()
        {
            Assert.Null(Context().get("nothingIsCalledThis"));
        }

        /// <summary>
        /// The statement's token arrives as the flag Calcite's side polls.
        /// </summary>
        /// <remarks>
        /// The same adaptation the time zone and the locale get: a .NET-side fact of the statement is put
        /// into the map in the form Calcite's generated code reads it. The flag has to be a registration
        /// rather than an <c>AtomicBoolean</c> that answers from the token, because <c>AtomicBoolean.get()</c>
        /// is <c>final</c> — measured against the IKVM assembly.
        ///
        /// <para>What reads it is a table: <c>ListTransientTable</c>, and the CSV, file and Kafka adapters'
        /// tables. No operator of <c>EnumerableDefaults</c> polls it for them, so the flag reaches exactly as
        /// far as the tables that read it.</para>
        /// </remarks>
        [Fact]
        public void Should_answer_a_cancel_flag_that_follows_the_statements_token()
        {
            using var cancellation = new System.Threading.CancellationTokenSource();
            using var context = Context(cancellationToken: cancellation.Token);

            var flag = (java.util.concurrent.atomic.AtomicBoolean)DataContext.Variable.CANCEL_FLAG.get(context);
            flag.Should().NotBeNull();
            flag.get().Should().BeFalse();

            cancellation.Cancel();

            flag.get().Should().BeTrue();
        }

        /// <summary>
        /// Disposing the context releases the registration, so a long-lived token stops holding the flag.
        /// </summary>
        /// <remarks>
        /// A caller's token outlives a statement — a request token runs many of them — so a registration
        /// left behind is one live callback and one flag held per statement for the life of that token.
        /// </remarks>
        [Fact]
        public void Should_release_the_cancel_flag_when_disposed()
        {
            using var cancellation = new System.Threading.CancellationTokenSource();
            var context = Context(cancellationToken: cancellation.Token);

            var flag = (java.util.concurrent.atomic.AtomicBoolean)DataContext.Variable.CANCEL_FLAG.get(context);
            context.Dispose();

            cancellation.Cancel();

            flag.get().Should().BeFalse("the registration went with the context");
        }

        /// <summary>
        /// A statement with no cancellation registers nothing, and still has a flag.
        /// </summary>
        /// <remarks>
        /// Calcite's own <c>CalciteConnectionImpl.createDataContext</c> always puts one in the map, and a
        /// table reads it without asking whether anybody can set it.
        /// </remarks>
        [Fact]
        public void Should_answer_a_cancel_flag_for_an_uncancellable_statement()
        {
            using var context = Context();

            var flag = (java.util.concurrent.atomic.AtomicBoolean)DataContext.Variable.CANCEL_FLAG.get(context);
            flag.Should().NotBeNull();
            flag.get().Should().BeFalse();
        }

    }

}
