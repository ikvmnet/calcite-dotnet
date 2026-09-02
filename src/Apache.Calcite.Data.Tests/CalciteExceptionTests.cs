using System;
using System.Data.Common;

using Apache.Calcite.Data.Internal;


using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteExceptionTests
    {

        [Fact]
        public void Should_be_DbException()
        {
            Assert.IsAssignableFrom<DbException>(new CalciteException());
        }

        [Fact]
        public void Should_preserve_message()
        {
            var e = new CalciteException("boom");
            Assert.Equal("boom", e.Message);
        }

        [Fact]
        public void Should_preserve_inner_exception()
        {
            var inner = new InvalidOperationException("inner");
            var e = new CalciteException("outer", inner);
            Assert.Same(inner, e.InnerException);
        }


        /// <summary>
        /// The JDK's reflection wrappers keep their cause out of <see cref="Exception.InnerException"/>:
        /// each calls <c>super((Throwable) null)</c> and overrides <c>getCause()</c> instead, so a .NET
        /// report of one carries no message and no cause.
        /// </summary>
        [Theory]
        [MemberData(nameof(Cause_hiding_wrappers))]
        public void Should_name_a_cause_dotnet_cannot_see(java.lang.Throwable wrapper)
        {
            Assert.Null(((Exception)wrapper).InnerException);

            var message = JavaCauses.Amend("Failed to execute Calcite statement.", wrapper);

            Assert.Contains("the real reason", message);
            Assert.StartsWith("Failed to execute Calcite statement.", message);
        }

        public static TheoryData<java.lang.Throwable> Cause_hiding_wrappers() => new()
        {
            new java.lang.reflect.UndeclaredThrowableException(new java.lang.RuntimeException("the real reason")),
            new java.lang.reflect.InvocationTargetException(new java.lang.RuntimeException("the real reason")),
            new java.lang.ExceptionInInitializerError(new java.lang.RuntimeException("the real reason")),
        };

        /// <summary>
        /// A cause .NET can already see is left alone; repeating it would double every report.
        /// </summary>
        [Fact]
        public void Should_leave_a_visible_cause_alone()
        {
            var visible = new java.lang.RuntimeException("outer", new java.lang.RuntimeException("the real reason"));
            Assert.NotNull(((Exception)visible).InnerException);

            Assert.Equal("boom", JavaCauses.Amend("boom", visible));
        }

        /// <summary>
        /// A wrapper reached through a cause .NET can see is still hidden from the report, so the walk
        /// follows the visible chain rather than only looking at its head.
        /// </summary>
        [Fact]
        public void Should_name_a_cause_hidden_beneath_a_visible_one()
        {
            var hidden = new java.lang.reflect.UndeclaredThrowableException(new java.lang.RuntimeException("the real reason"));
            var outer = new java.lang.IllegalStateException("unable to implement", hidden);

            Assert.Contains("the real reason", JavaCauses.Amend("boom", outer));
        }

        /// <summary>
        /// Neither a plain CLR exception nor no exception at all is a Java throwable, and neither may
        /// change the message.
        /// </summary>
        [Fact]
        public void Should_leave_a_clr_exception_alone()
        {
            Assert.Equal("boom", JavaCauses.Amend("boom", new InvalidOperationException("clr only")));
            Assert.Equal("boom", JavaCauses.Amend("boom", null));
        }

    }

}
