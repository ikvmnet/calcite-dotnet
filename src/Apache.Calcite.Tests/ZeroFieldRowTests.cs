using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Holds the two constants a row of no fields is, and the fact about IKVM that had them resolving to
    /// nothing.
    /// </summary>
    /// <remarks>
    /// <c>FlatLists.COMPARABLE_EMPTY_LIST</c> and <c>Unit.INSTANCE</c> are Java <c>static final</c> fields,
    /// and <b>IKVM does not compile one to a CLR field of that name</b> — it emits a property over a backing
    /// field it renames to <c>__&lt;&gt;NAME</c>, so that reading it from C# runs the class initializer the
    /// way Java guarantees. <c>GetField</c> by the Java name answers nothing, and both of these were looked
    /// up that way behind a <c>!</c>: null since they were written, and an NRE waiting for the first query
    /// whose row type has no fields.
    ///
    /// <para>Nothing had ever reached that shape, which is why the suite was green over a null. These
    /// evaluate the expressions rather than assert on their node type, because what was wrong was the
    /// resolution and only running it settles that.</para>
    /// </remarks>
    [TestClass]
    public class ZeroFieldRowTests
    {

        /// <summary>
        /// The row a LIST format builds for a type with no fields.
        /// </summary>
        [TestMethod]
        public void ShouldBuildAnEmptyComparableListRow()
        {
            var row = Evaluate(JavaRowFormat.LIST.Record(typeof(java.util.List), []));

            row.Should().BeSameAs(org.apache.calcite.runtime.FlatLists.COMPARABLE_EMPTY_LIST);
            ((java.util.List)row).size().Should().Be(0);
        }

        /// <summary>
        /// The row a CUSTOM format builds for a type with no fields, which is Calcite's unit value rather
        /// than an instance of the record class.
        /// </summary>
        [TestMethod]
        public void ShouldBuildAUnitRow()
        {
            var row = Evaluate(JavaRowFormat.CUSTOM.Record(typeof(object), []));

            row.Should().BeSameAs(org.apache.calcite.runtime.Unit.INSTANCE);
        }

        /// <summary>
        /// The selector a physical type of no fields generates, which is the path a plan actually reaches
        /// the constant through.
        /// </summary>
        [TestMethod]
        public void ShouldGenerateASelectorForARowOfNoFields()
        {
            // two fields, so that the optimising overload leaves the format ARRAY rather than collapsing it
            // to the field's own type
            var typeFactory = new JavaTypeFactoryImpl();
            var rowType = typeFactory.builder()
                .add("A", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER))
                .add("B", typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.VARCHAR))
                .build();

            var physType = ClrPhysTypeImpl.Of(typeFactory, rowType, JavaRowFormat.ARRAY);
            var parameter = Expression.Parameter(physType.RowType, "v1");
            var selector = physType.GenerateSelector(parameter, new java.util.ArrayList(), JavaRowFormat.LIST);

            var row = selector.Compile().DynamicInvoke([new object[] { java.lang.Integer.valueOf(1), "x" }]);

            row.Should().BeSameAs(org.apache.calcite.runtime.FlatLists.COMPARABLE_EMPTY_LIST);
        }

        /// <summary>
        /// Runs an expression that takes nothing.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static object Evaluate(Expression expression)
        {
            return Expression.Lambda<Func<object>>(Expression.Convert(expression, typeof(object))).Compile()();
        }

    }

}
