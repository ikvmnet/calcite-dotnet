using System;

using Xunit;

namespace Apache.Calcite.Data.Common.Tests
{

    public class BigDecimalConverterTests
    {

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
        public void Decimal_should_roundtrip_through_BigDecimal(string literal)
        {
            var value = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

            var bd = BigDecimalConverter.ToBigDecimal(value);
            var back = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(value, back);
        }

        [Theory]
        [InlineData("0", 0)]
        [InlineData("1", 0)]
        [InlineData("-1", 0)]
        [InlineData("123.456", 3)]
        [InlineData("-123.456", 3)]
        [InlineData("0.0000000000000000000000000001", 28)]
        [InlineData("79228162514264337593543950335", 0)]
        public void ToBigDecimal_should_preserve_scale_and_value(string literal, int expectedScale)
        {
            var value = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

            var bd = BigDecimalConverter.ToBigDecimal(value);

            Assert.Equal(expectedScale, bd.scale());
            Assert.Equal(literal, bd.toPlainString());
        }

        [Theory]
        [InlineData("0", 0)]
        [InlineData("1", 0)]
        [InlineData("-1", 0)]
        [InlineData("123.456", 3)]
        [InlineData("-0.001", 3)]
        [InlineData("79228162514264337593543950335", 0)]
        [InlineData("7.9228162514264337593543950335", 28)]
        public void ToDecimal_should_match_BigDecimal_value(string literal, int scale)
        {
            var bd = new java.math.BigDecimal(literal);
            Assert.Equal(scale, bd.scale());

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture), d);
        }

        [Fact]
        public void ToDecimal_should_normalize_negative_scale()
        {
            // 12e2 => unscaled=12, scale=-2 => value 1200, normalized to scale 0.
            var bd = new java.math.BigDecimal(java.math.BigInteger.valueOf(12L), -2);

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(1200m, d);
        }

        [Fact]
        public void ToDecimal_should_round_half_even_to_even_when_scale_exceeds_28()
        {
            // 29 fractional digits, drop trailing '5' => exactly half. Preceding '8' is even -> stay.
            var bd = new java.math.BigDecimal("0.12345678901234567890123456785");

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(0.1234567890123456789012345678m, d);
        }

        [Fact]
        public void ToDecimal_should_round_half_even_up_when_preceding_digit_is_odd()
        {
            // 29 fractional digits, drop trailing '5' => exactly half. Preceding '7' is odd -> round up.
            var bd = new java.math.BigDecimal("0.12345678901234567890123456775");

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(0.1234567890123456789012345678m, d);
        }

        [Fact]
        public void ToDecimal_should_throw_when_magnitude_exceeds_decimal_range()
        {
            // 2^96 exceeds 96-bit decimal mantissa.
            var unscaled = java.math.BigInteger.valueOf(2L).pow(96);
            var bd = new java.math.BigDecimal(unscaled, 0);

            Assert.Throws<OverflowException>(() => BigDecimalConverter.ToDecimal(bd));
        }

        [Fact]
        public void ToDecimal_should_handle_max_96_bit_magnitude()
        {
            // 2^96 - 1 == decimal.MaxValue when scale is 0.
            var unscaled = java.math.BigInteger.valueOf(2L).pow(96).subtract(java.math.BigInteger.ONE);
            var bd = new java.math.BigDecimal(unscaled, 0);

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(decimal.MaxValue, d);
        }

        [Fact]
        public void ToDecimal_should_handle_negative_max_96_bit_magnitude()
        {
            var unscaled = java.math.BigInteger.valueOf(2L).pow(96).subtract(java.math.BigInteger.ONE).negate();
            var bd = new java.math.BigDecimal(unscaled, 0);

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(decimal.MinValue, d);
        }

        [Fact]
        public void ToBigDecimal_zero_should_have_signum_zero()
        {
            var bd = BigDecimalConverter.ToBigDecimal(0m);

            Assert.Equal(0, bd.signum());
            Assert.Equal(0, bd.scale());
        }

        [Fact]
        public void ToBigDecimal_negative_zero_with_scale_should_preserve_scale()
        {
            // 0.00m is represented as mantissa 0, scale 2.
            var value = 0.00m;

            var bd = BigDecimalConverter.ToBigDecimal(value);

            Assert.Equal(0, bd.signum());
            Assert.Equal(2, bd.scale());
            Assert.Equal(0m, BigDecimalConverter.ToDecimal(bd));
        }

        [Fact]
        public void ToDecimal_zero_with_large_scale_should_return_zero()
        {
            var bd = new java.math.BigDecimal(java.math.BigInteger.ZERO, 50);

            var d = BigDecimalConverter.ToDecimal(bd);

            Assert.Equal(0m, d);
        }

        [Theory]
        [InlineData("1.23", "1.23")]
        [InlineData("-1.23", "-1.23")]
        [InlineData("123456789012345678901234567.89", "123456789012345678901234567.89")]
        public void Roundtrip_should_preserve_string_representation(string input, string expected)
        {
            var d = decimal.Parse(input, System.Globalization.CultureInfo.InvariantCulture);

            var bd = BigDecimalConverter.ToBigDecimal(d);

            Assert.Equal(expected, bd.toPlainString());
            Assert.Equal(d, BigDecimalConverter.ToDecimal(bd));
        }

    }

}
