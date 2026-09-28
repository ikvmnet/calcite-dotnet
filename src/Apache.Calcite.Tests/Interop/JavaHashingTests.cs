using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;

using FluentAssertions;

using Xunit;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    /// <summary>
    /// Checks that a string hashes on the Java side to the value Java specifies, so a <c>java.util.HashMap</c>
    /// iterates in the same order in every process.
    /// </summary>
    /// <remarks>
    /// Java specifies <c>String.hashCode</c> as <c>s[0]*31^(n-1) + …</c>; the CLR's string hash is randomised
    /// per process. Operators such as <c>ClrCursorDefaults.GroupBy</c> and <c>ClrCursorDefaults.AsofJoin</c>
    /// hold rows in a <c>java.util.HashMap</c> so that the order of rows a query does not sort matches
    /// <c>EnumerableConvention</c>, which depends on IKVM using Java's hash for a string.
    /// </remarks>
    public class JavaHashingTests
    {

        /// <summary>
        /// A string hashes on the Java side to the value the Java language specifies.
        /// </summary>
        [Fact]
        public void ShouldHashAStringTheWayJavaSpecifies()
        {
            java.util.Objects.hashCode("EAST").Should().Be(2120701);
            java.util.Objects.hashCode("WEST").Should().Be(2660783);
            java.util.Objects.hashCode("").Should().Be(0);
        }

        /// <summary>
        /// A <c>java.util.HashMap</c> of strings iterates in a fixed order, the one Java's string hash gives,
        /// rather than one that varies with the CLR's per-process hash.
        /// </summary>
        [Fact]
        public void ShouldIterateAHashMapInTheSameOrderEveryRun()
        {
            var map = new java.util.HashMap();
            foreach (var s in new[] { "EAST", "WEST", "NORTH", "SOUTH", "A", "B", "C", "D" })
                map.put(s, s);

            var order = new System.Text.StringBuilder();
            for (var i = map.keySet().iterator(); i.hasNext();)
                order.Append((string)i.next()).Append(',');

            order.ToString().Should().Be("A,B,C,D,NORTH,WEST,SOUTH,EAST,");
        }

    }

}
