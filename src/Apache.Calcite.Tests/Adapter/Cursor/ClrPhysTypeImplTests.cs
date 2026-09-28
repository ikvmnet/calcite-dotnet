using System;
using System.Linq.Expressions;


using FluentAssertions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;

using Xunit;

namespace Apache.Calcite.Extensions.Adapter.Cursor.Tests
{

    /// <summary>
    /// Tests of the rows <c>ClrPhysTypeImpl</c> and <c>JavaRowFormatExtensions</c> build for a row type with no
    /// fields.
    /// </summary>
    /// <remarks>
    /// Such a row is one of two Java constants, <c>FlatLists.COMPARABLE_EMPTY_LIST</c> or <c>Unit.INSTANCE</c>.
    /// IKVM compiles a Java <c>static final</c> field to a property over a renamed backing field, so the
    /// expression must reach it as a property rather than a field of the Java name. These tests evaluate the
    /// expressions, because only evaluating them shows that the member resolved.
    /// </remarks>
    public class ClrPhysTypeImplTests
    {

        /// <summary>
        /// The row a LIST format builds for a type with no fields.
        /// </summary>
        [Fact]
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
        [Fact]
        public void ShouldBuildAUnitRow()
        {
            var row = Evaluate(JavaRowFormat.CUSTOM.Record(typeof(object), []));

            row.Should().BeSameAs(org.apache.calcite.runtime.Unit.INSTANCE);
        }

        /// <summary>
        /// A selector projecting no fields builds the empty-list row; this is the route by which a plan reaches
        /// the constant.
        /// </summary>
        [Fact]
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
        /// Compiles and runs an expression that takes no parameters, returning its value.
        /// </summary>
        /// <param name="expression">An expression with no free parameters.</param>
        /// <returns>The expression's value, boxed.</returns>
        static object Evaluate(Expression expression)
        {
            return Expression.Lambda<Func<object>>(Expression.Convert(expression, typeof(object))).Compile()();
        }

    }

}
