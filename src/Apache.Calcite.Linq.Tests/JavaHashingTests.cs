using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// What a string hashes to, and what a <c>java.util.HashMap</c> therefore iterates in.
    /// </summary>
    /// <remarks>
    /// This convention has to agree with <c>EnumerableConvention</c> on the order of rows a query does not
    /// order, and where that order is a map's it is the map's hashing that decides it. Java <i>specifies</i>
    /// <c>String.hashCode</c> — <c>s[0]*31^(n-1) + …</c> — so it is the same on every JVM and every run. The
    /// CLR's is randomised per process, and if IKVM handed a string to it, no two runs would agree.
    ///
    /// <para>These tests say it does not: what a string hashes to on the Java side is Java's value. That is
    /// why holding rows in a <c>java.util.HashMap</c>, as <c>ClrEnumerableDefaults.AsofJoin</c> and
    /// <c>ClrEnumerableDefaults.Window</c> do, reproduces Calcite's order rather than merely happening to
    /// match it.</para>
    /// </remarks>
    [TestClass]
    public class JavaHashingTests
    {

        /// <summary>
        /// A string hashes on the Java side to what the Java language specifies.
        /// </summary>
        [TestMethod]
        public void ShouldHashAStringTheWayJavaSpecifies()
        {
            java.util.Objects.hashCode("EAST").Should().Be(2120701);
            java.util.Objects.hashCode("WEST").Should().Be(2660783);
            java.util.Objects.hashCode("").Should().Be(0);
        }

        /// <summary>
        /// A <c>java.util.HashMap</c> iterates in the same order in every process, because that order follows
        /// from the hash above and not from the CLR's.
        /// </summary>
        [TestMethod]
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
