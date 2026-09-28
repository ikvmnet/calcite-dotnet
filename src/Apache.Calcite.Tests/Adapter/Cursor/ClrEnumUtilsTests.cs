using System;
using System.Linq.Expressions;


using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of the conversions <c>ClrEnumUtils</c> writes into a plan.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumUtilsTest</c>, which asserts on the text of the generated tree. An
    /// <see cref="Expression"/> tree has no comparable rendering, so these compile the conversion and check the
    /// values it produces.
    /// </remarks>
    public class ClrEnumUtilsTests
    {

        /// <summary>
        /// A value typed as <see cref="object"/> and wanted as a <c>java.lang.Number</c> is converted rather than
        /// cast, so the string <c>"100"</c> becomes a <c>BigDecimal</c>.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumUtilsTest.testObjectToNumberConvert</c>. A string bound to a parameter that is compared
        /// against an integer column reaches this conversion.
        /// </remarks>
        [Fact]
        public void ShouldConvertAnObjectToANumber()
        {
            var x = Expression.Parameter(typeof(object), "x");
            var convert = Expression.Lambda<Func<object, java.lang.Number>>(ClrEnumUtils.Convert(x, typeof(java.lang.Number)), x).Compile();

            convert("100").Should().Be(new java.math.BigDecimal("100"));
            convert(java.lang.Integer.valueOf(100)).Should().Be(new java.math.BigDecimal("100"));
            convert(null).Should().BeNull();
        }

        /// <inheritdoc cref="ShouldConvertAnObjectToANumber" />
        [Fact]
        public void ShouldConvertAStringToANumber()
        {
            var s = Expression.Parameter(typeof(string), "s");
            var convert = Expression.Lambda<Func<string, java.lang.Number>>(ClrEnumUtils.Convert(s, typeof(java.lang.Number)), s).Compile();

            convert("100").Should().Be(new java.math.BigDecimal("100"));
            convert(null).Should().BeNull();
        }

        /// <summary>
        /// Converting a value that is not a number throws <c>NumberFormatException</c> naming the value, rather
        /// than a cast exception naming only the two types.
        /// </summary>
        [Fact]
        public void ShouldNameTheValueThatIsNotANumber()
        {
            var x = Expression.Parameter(typeof(object), "x");
            var convert = Expression.Lambda<Func<object, java.lang.Number>>(ClrEnumUtils.Convert(x, typeof(java.lang.Number)), x).Compile();

            var thrown = Assert.ThrowsAny<java.lang.NumberFormatException>(() => convert("abc"));
            thrown.getMessage().Should().Contain("abc");
        }

    }

}
