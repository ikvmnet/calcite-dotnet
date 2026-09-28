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
    /// The variables a statement reads through its <c>DataContext</c>, which should match those
    /// <c>CalciteConnectionImpl.DataContextImpl</c> puts, including the time-zone offsets on the timestamps.
    /// </summary>
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
        /// A connection's time zone offsets <c>CURRENT_TIMESTAMP</c> and <c>LOCAL_TIMESTAMP</c> from
        /// <c>UTC_TIMESTAMP</c> by that zone's offset.
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
        /// On a UTC connection <c>CURRENT_TIMESTAMP</c> equals <c>UTC_TIMESTAMP</c>.
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
        /// Cancelling the statement's token sets the <c>CANCEL_FLAG</c> that Calcite's tables poll.
        /// </summary>
        /// <remarks>
        /// The flag is an <c>AtomicBoolean</c> set by a registration on the token, because
        /// <c>AtomicBoolean.get()</c> is <c>final</c> and a subclass cannot answer from the token directly.
        /// Only tables read the flag (<c>ListTransientTable</c> and the CSV, file and Kafka adapters' tables);
        /// no operator of <c>EnumerableDefaults</c> polls it.
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
        /// A caller's token can outlive many statements, and each registration left on it would keep a
        /// callback and a flag alive for the token's lifetime.
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
        /// A statement with no cancellation still has a flag, as Calcite's <c>DataContextImpl</c> always puts
        /// one.
        /// </summary>
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
