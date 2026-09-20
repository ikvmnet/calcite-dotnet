using System;
using System.Globalization;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    public class JavaDecimalsTests
    {

        static decimal Parse(string literal)
        {
            return decimal.Parse(literal, CultureInfo.InvariantCulture);
        }

        [Theory]
        [InlineData("0")]
        [InlineData("1")]
        [InlineData("-1")]
        [InlineData("0.0")]
        [InlineData("0.0000000000000000000000000001")] // smallest positive at scale 28
        [InlineData("-0.0000000000000000000000000001")]
        [InlineData("123.456")]
        [InlineData("-123.456")]
        [InlineData("79228162514264337593543950335")] // decimal.MaxValue
        [InlineData("-79228162514264337593543950335")] // decimal.MinValue
        [InlineData("7.9228162514264337593543950335")] // MaxValue with scale 28
        [InlineData("-7.9228162514264337593543950335")]
        [InlineData("12345678901234567890.123456789")] // scale 9, large mantissa
        [InlineData("0.5")]
        [InlineData("-0.5")]
        [InlineData("100000000000000000000")] // 10^20, fits in 96 bits, scale 0
        public void ShouldRoundTripADecimalThroughBigDecimal(string literal)
        {
            var value = Parse(literal);

            var bd = JavaDecimals.ToBigDecimal(value);
            var back = JavaDecimals.ToDecimal(bd);

            back.Should().Be(value);
        }

        [Theory]
        [InlineData("0", 0)]
        [InlineData("1", 0)]
        [InlineData("-1", 0)]
        [InlineData("123.456", 3)]
        [InlineData("-123.456", 3)]
        [InlineData("0.0000000000000000000000000001", 28)]
        [InlineData("79228162514264337593543950335", 0)]
        public void ShouldPreserveScaleAndValue(string literal, int expectedScale)
        {
            var bd = JavaDecimals.ToBigDecimal(Parse(literal));

            bd.scale().Should().Be(expectedScale);
            bd.toPlainString().Should().Be(literal);
        }

        [Theory]
        [InlineData("0", 0)]
        [InlineData("1", 0)]
        [InlineData("-1", 0)]
        [InlineData("123.456", 3)]
        [InlineData("-0.001", 3)]
        [InlineData("79228162514264337593543950335", 0)]
        [InlineData("7.9228162514264337593543950335", 28)]
        public void ShouldMatchTheBigDecimalValue(string literal, int scale)
        {
            var bd = new java.math.BigDecimal(literal);
            bd.scale().Should().Be(scale);

            JavaDecimals.ToDecimal(bd).Should().Be(Parse(literal));
        }

        /// <summary>
        /// The values whose <c>toString()</c> is scientific notation, which is what the text route could not
        /// read: an adjusted exponent below -6, or a negative scale.
        /// </summary>
        [Theory]
        [InlineData("0.0000001", "1E-7")]
        [InlineData("0.00000012345", "1.2345E-7")]
        [InlineData("1E+10", "1E+10")]
        public void ShouldReadAValueBigDecimalWritesWithAnExponent(string literal, string written)
        {
            var bd = new java.math.BigDecimal(literal);
            bd.toString().Should().Be(written, "the text route reads this and there is no exponent in NumberStyles.Number");

            JavaDecimals.ToDecimal(bd).Should().Be(Parse(bd.toPlainString()));
        }

        [Fact]
        public void ShouldNormalizeANegativeScale()
        {
            // 12e2 => unscaled=12, scale=-2 => value 1200, normalized to scale 0.
            var bd = new java.math.BigDecimal(java.math.BigInteger.valueOf(12L), -2);

            JavaDecimals.ToDecimal(bd).Should().Be(1200m);
        }

        [Fact]
        public void ShouldRoundHalfEvenToEvenWhenScaleExceeds28()
        {
            // 29 fractional digits, drop trailing '5' => exactly half. Preceding '8' is even -> stay.
            var bd = new java.math.BigDecimal("0.12345678901234567890123456785");

            JavaDecimals.ToDecimal(bd).Should().Be(0.1234567890123456789012345678m);
        }

        [Fact]
        public void ShouldRoundHalfEvenUpWhenThePrecedingDigitIsOdd()
        {
            // 29 fractional digits, drop trailing '5' => exactly half. Preceding '7' is odd -> round up.
            var bd = new java.math.BigDecimal("0.12345678901234567890123456775");

            JavaDecimals.ToDecimal(bd).Should().Be(0.1234567890123456789012345678m);
        }

        [Fact]
        public void ShouldThrowWhenTheMagnitudeExceedsDecimalRange()
        {
            // 2^96 exceeds the 96-bit decimal mantissa.
            var bd = new java.math.BigDecimal(java.math.BigInteger.valueOf(2L).pow(96), 0);

            Assert.ThrowsAny<OverflowException>(() => JavaDecimals.ToDecimal(bd));
        }

        [Fact]
        public void ShouldHandleTheMaximum96BitMagnitude()
        {
            // 2^96 - 1 == decimal.MaxValue when the scale is 0.
            var unscaled = java.math.BigInteger.valueOf(2L).pow(96).subtract(java.math.BigInteger.ONE);

            JavaDecimals.ToDecimal(new java.math.BigDecimal(unscaled, 0)).Should().Be(decimal.MaxValue);
        }

        [Fact]
        public void ShouldHandleTheNegativeMaximum96BitMagnitude()
        {
            var unscaled = java.math.BigInteger.valueOf(2L).pow(96).subtract(java.math.BigInteger.ONE).negate();

            JavaDecimals.ToDecimal(new java.math.BigDecimal(unscaled, 0)).Should().Be(decimal.MinValue);
        }

        [Fact]
        public void ZeroShouldHaveSignumZero()
        {
            var bd = JavaDecimals.ToBigDecimal(0m);

            bd.signum().Should().Be(0);
            bd.scale().Should().Be(0);
        }

        [Fact]
        public void AZeroCarryingAScaleShouldKeepIt()
        {
            // 0.00m is represented as mantissa 0, scale 2.
            var bd = JavaDecimals.ToBigDecimal(0.00m);

            bd.signum().Should().Be(0);
            bd.scale().Should().Be(2);
            JavaDecimals.ToDecimal(bd).Should().Be(0m);
        }

        [Fact]
        public void AZeroWithALargeScaleShouldReadAsZero()
        {
            var bd = new java.math.BigDecimal(java.math.BigInteger.ZERO, 50);

            JavaDecimals.ToDecimal(bd).Should().Be(0m);
        }

        [Theory]
        [InlineData("1.23")]
        [InlineData("-1.23")]
        [InlineData("123456789012345678901234567.89")]
        public void ARoundTripShouldPreserveTheStringRepresentation(string literal)
        {
            var value = Parse(literal);

            var bd = JavaDecimals.ToBigDecimal(value);

            bd.toPlainString().Should().Be(literal);
            JavaDecimals.ToDecimal(bd).Should().Be(value);
        }

    }

}
