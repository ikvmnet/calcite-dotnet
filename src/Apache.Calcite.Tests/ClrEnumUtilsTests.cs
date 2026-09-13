using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The conversions <c>ClrEnumUtils</c> writes into a plan.
    /// </summary>
    /// <remarks>
    /// <c>EnumUtilsTest</c>, which reads the tree a conversion produced as text. A tree of
    /// <see cref="Expression"/> has no such rendering that is worth asserting against, so these compile it and
    /// read what it does — which is the thing the conversion exists for.
    /// </remarks>
    [TestClass]
    public class ClrEnumUtilsTests
    {

        /// <summary>
        /// A value known only as an object, wanted as a number, is converted rather than cast.
        /// </summary>
        /// <remarks>
        /// <c>EnumUtilsTest.testObjectToNumberConvert</c>, and CALCITE-6284: binding a string to a parameter
        /// compared against an integer column reaches this, and a cast answered
        /// <c>ClassCastException</c> — here, <c>InvalidCastException</c> — instead of saying which value was
        /// not a number.
        /// </remarks>
        [TestMethod]
        public void ShouldConvertAnObjectToANumber()
        {
            var x = Expression.Parameter(typeof(object), "x");
            var convert = Expression.Lambda<Func<object, java.lang.Number>>(ClrEnumUtils.Convert(x, typeof(java.lang.Number)), x).Compile();

            convert("100").Should().Be(new java.math.BigDecimal("100"));
            convert(java.lang.Integer.valueOf(100)).Should().Be(new java.math.BigDecimal("100"));
            convert(null).Should().BeNull();
        }

        /// <inheritdoc cref="ShouldConvertAnObjectToANumber" />
        [TestMethod]
        public void ShouldConvertAStringToANumber()
        {
            var s = Expression.Parameter(typeof(string), "s");
            var convert = Expression.Lambda<Func<string, java.lang.Number>>(ClrEnumUtils.Convert(s, typeof(java.lang.Number)), s).Compile();

            convert("100").Should().Be(new java.math.BigDecimal("100"));
            convert(null).Should().BeNull();
        }

        /// <summary>
        /// A value that is not a number says so.
        /// </summary>
        /// <remarks>
        /// The other half of CALCITE-6284: the failure names the value, where a cast named only the two types.
        /// </remarks>
        [TestMethod]
        public void ShouldNameTheValueThatIsNotANumber()
        {
            var x = Expression.Parameter(typeof(object), "x");
            var convert = Expression.Lambda<Func<object, java.lang.Number>>(ClrEnumUtils.Convert(x, typeof(java.lang.Number)), x).Compile();

            var thrown = Assert.Throws<java.lang.NumberFormatException>(() => convert("abc"));
            thrown.getMessage().Should().Contain("abc");
        }

    }

}
