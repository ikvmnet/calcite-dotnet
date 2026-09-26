using System;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    /// <summary>
    /// The adapter every value crossing between the two runtimes goes through, both ways.
    /// </summary>
    /// <remarks>
    /// A CLR primitive handed to Calcite has to be the box Java gives it, and a Java box read as a CLR
    /// primitive has to be unboxed by its accessor, or one value has two representations in a plan and
    /// Calcite's comparators fail on them. These hold each of Java's eight primitives both ways, the value
    /// types that are not Java primitives passing through, and the one case the adapter answers by
    /// reflection: a value of another primitive than the one it is boxed as, which
    /// <c>MethodBase.Invoke</c> widens.
    /// </remarks>
    public class JavaValuesTests
    {

        /// <summary>
        /// Each Java primitive is boxed as Java boxes it.
        /// </summary>
        [Fact]
        public void ShouldBoxEachPrimitiveAsJavaDoes()
        {
            JavaValues.From(3).Should().BeOfType<java.lang.Integer>().Which.intValue().Should().Be(3);
            JavaValues.From(4L).Should().BeOfType<java.lang.Long>().Which.longValue().Should().Be(4L);
            JavaValues.From(6.5d).Should().BeOfType<java.lang.Double>().Which.doubleValue().Should().Be(6.5d);
            JavaValues.From(5.5f).Should().BeOfType<java.lang.Float>().Which.floatValue().Should().Be(5.5f);
            JavaValues.From((short)2).Should().BeOfType<java.lang.Short>().Which.shortValue().Should().Be(2);
            JavaValues.From((byte)200).Should().BeOfType<java.lang.Byte>().Which.byteValue().Should().Be(200);
            JavaValues.From('x').Should().BeOfType<java.lang.Character>().Which.charValue().Should().Be('x');
            JavaValues.From(true).Should().BeOfType<java.lang.Boolean>().Which.booleanValue().Should().BeTrue();
        }

        /// <summary>
        /// A value type that is not one of Java's primitives, and any reference, is returned as it is.
        /// </summary>
        [Fact]
        public void ShouldPassEverythingElseThrough()
        {
            JavaValues.From(1.5m).Should().Be(1.5m);
            JavaValues.From((sbyte)-1).Should().BeOfType<sbyte>();
            JavaValues.From(7u).Should().BeOfType<uint>();

            var guid = Guid.NewGuid();
            JavaValues.From(guid).Should().Be(guid);

            var box = java.lang.Integer.valueOf(9);
            JavaValues.From<object>(box).Should().BeSameAs(box);
            JavaValues.From("a").Should().Be("a");
            JavaValues.From<object?>(null).Should().BeNull();
        }

        /// <summary>
        /// Each Java box is unboxed by its accessor, and any <c>Number</c> answers a numeric one.
        /// </summary>
        [Fact]
        public void ShouldUnboxEachPrimitiveByItsAccessor()
        {
            JavaValues.Unwrap(java.lang.Integer.valueOf(3), typeof(int)).Should().Be(3);
            JavaValues.Unwrap(java.lang.Long.valueOf(4L), typeof(long)).Should().Be(4L);
            JavaValues.Unwrap(java.lang.Double.valueOf(6.5d), typeof(double)).Should().Be(6.5d);
            JavaValues.Unwrap(java.lang.Float.valueOf(5.5f), typeof(float)).Should().Be(5.5f);
            JavaValues.Unwrap(java.lang.Short.valueOf(2), typeof(short)).Should().Be((short)2);
            JavaValues.Unwrap(java.lang.Byte.valueOf(200), typeof(byte)).Should().Be((byte)200);
            JavaValues.Unwrap(java.lang.Character.valueOf('x'), typeof(char)).Should().Be('x');
            JavaValues.Unwrap(java.lang.Boolean.valueOf(true), typeof(bool)).Should().Be(true);

            // the accessor, not the box's own type: an Integer read as a long, a BigDecimal read as an int
            JavaValues.Unwrap(java.lang.Integer.valueOf(3), typeof(long)).Should().Be(3L);
            JavaValues.Unwrap(new java.math.BigDecimal("12.75"), typeof(int)).Should().Be(12);
        }

        /// <summary>
        /// A type that is not a Java primitive, or a value without the accessor, is refused.
        /// </summary>
        [Fact]
        public void ShouldRefuseWhatHasNoAccessor()
        {
            var notPrimitive = () => JavaValues.Unwrap(java.lang.Integer.valueOf(3), typeof(decimal));
            notPrimitive.Should().Throw<NotSupportedException>().WithMessage("*not a Java primitive*");

            var noAccessor = () => JavaValues.Unwrap("3", typeof(int));
            noAccessor.Should().Throw<NotSupportedException>().WithMessage("*has no intValue()*");

            var clrBox = () => JavaValues.Unwrap(3, typeof(int));
            clrBox.Should().Throw<NotSupportedException>().WithMessage("*has no intValue()*");
        }

        /// <summary>
        /// Reading at a type converts in whichever direction the value needs.
        /// </summary>
        [Fact]
        public void ShouldConvertEitherWayThroughAs()
        {
            JavaValues.As<int>(java.lang.Integer.valueOf(3)).Should().Be(3);
            JavaValues.As<java.lang.Integer>(3).intValue().Should().Be(3);
            JavaValues.As<int>(3).Should().Be(3);

            // a CLR int read where the row type is java.lang.Long: boxed by Long.valueOf(long), the int
            // widened on the way in, which is the one case the adapter still answers by reflection
            JavaValues.As<java.lang.Long>(5).longValue().Should().Be(5L);
        }

        /// <summary>
        /// A value of another primitive than the one it is boxed as is widened, as reflection widens it.
        /// </summary>
        [Fact]
        public void ShouldWidenAValueOfAnotherPrimitive()
        {
            JavaValues.Box(5, typeof(long)).Should().BeOfType<java.lang.Long>().Which.longValue().Should().Be(5L);
            JavaValues.Box((short)5, typeof(double)).Should().BeOfType<java.lang.Double>().Which.doubleValue().Should().Be(5d);

            var narrowing = () => JavaValues.Box(5L, typeof(int));
            narrowing.Should().Throw<ArgumentException>("reflection widens and never narrows");
        }

    }

}
