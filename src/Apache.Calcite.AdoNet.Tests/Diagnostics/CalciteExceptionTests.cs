using System;
using System.Data.Common;


using Xunit;

namespace Apache.Calcite.AdoNet.Tests.Diagnostics
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

    }

}
