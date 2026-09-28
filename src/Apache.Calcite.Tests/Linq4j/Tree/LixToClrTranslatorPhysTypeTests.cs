using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

using FluentAssertions;

using java.lang;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree.Tests
{

    /// <summary>
    /// Translates the linq4j expressions Calcite's <see cref="PhysTypeImpl"/> emits with
    /// <see cref="LixToClrTranslator"/>, and runs the result.
    /// </summary>
    public class LixToClrTranslatorPhysTypeTests
    {

        readonly JavaTypeFactoryImpl typeFactory;
        readonly RelDataType rowType;
        readonly PhysType physType;

        /// <summary>
        /// Builds a two-column <c>ARRAY</c> physical type of an <c>INTEGER</c> and a <c>VARCHAR</c>.
        /// </summary>
        public LixToClrTranslatorPhysTypeTests()
        {
            typeFactory = new JavaTypeFactoryImpl();
            rowType = typeFactory.builder()
                .add("EMPNO", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .build();
            physType = PhysTypeImpl.of(typeFactory, rowType, JavaRowFormat.ARRAY);
        }

        /// <summary>
        /// Translates and compiles a comparator expression, and returns a delegate that calls it.
        /// </summary>
        /// <param name="expression">A linq4j expression that evaluates to a <c>java.util.Comparator</c>.</param>
        /// <returns>A delegate that compares two rows through the translated comparator.</returns>
        static Func<object[], object[], int> Comparator(J.Expression expression)
        {
            var translated = new LixToClrTranslator().Translate(expression);

            translated.Type.Should().Be(typeof(java.util.Comparator), "an anonymous class is wrapped back into its interface");

            var comparator = Expression.Lambda<Func<java.util.Comparator>>(translated).Compile()();

            return (x, y) => comparator.compare(x, y);
        }

        [Fact]
        public void ShouldTranslateFieldReference()
        {
            var row = J.Expressions.parameter(physType.getJavaRowType(), "row");
            var target = Expression.Parameter(typeof(object[]), "row");

            var translator = new LixToClrTranslator();
            translator.Bind(row, target);

            var name = translator.Translate(physType.fieldReference(row, 1));
            var read = Expression.Lambda<Func<object[], object>>(Expression.Convert(name, typeof(object)), target).Compile();

            read([Integer.valueOf(7), "SMITH"]).Should().Be("SMITH");
        }

        [Fact]
        public void ShouldTranslateRecord()
        {
            var translator = new LixToClrTranslator();
            var record = translator.Translate(
                physType.record(java.util.Arrays.asList([
                    J.Expressions.constant(Integer.valueOf(7)),
                    J.Expressions.constant("SMITH")])));

            var build = Expression.Lambda<Func<object[]>>(record).Compile();

            build().Should().BeEquivalentTo(new object[] { Integer.valueOf(7), "SMITH" });
        }

        [Fact]
        public void ShouldTranslateSelector()
        {
            var row = J.Expressions.parameter(physType.getJavaRowType(), "row");
            var selector = physType.generateSelector(row, java.util.Arrays.asList([Integer.valueOf(1)]));

            var translated = AnonymousClasses.Unwrap(new LixToClrTranslator().Translate(selector))!;
            var apply = translated.Compile();

            apply.DynamicInvoke([new object[] { Integer.valueOf(7), "SMITH" }]).Should().Be("SMITH");
        }

        [Fact]
        public void ShouldTranslateComparator()
        {
            // an anonymous java.util.Comparator, which an expression tree cannot declare, becomes a lambda
            // for its compare method wrapped in an implementation of the interface
            var collation = RelCollations.of(0);
            var compare = Comparator(physType.generateComparator(collation));

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(2), "b"]).Should().BeNegative();
            compare([Integer.valueOf(2), "b"], [Integer.valueOf(1), "a"]).Should().BePositive();
            compare([Integer.valueOf(1), "a"], [Integer.valueOf(1), "z"]).Should().Be(0);
        }

        [Fact]
        public void ShouldTranslateDescendingComparator()
        {
            var collation = RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
            var compare = Comparator(physType.generateComparator(collation));

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(2), "b"]).Should().BePositive();
        }

        [Fact]
        public void ShouldSortWithATranslatedComparator()
        {
            var compare = Comparator(physType.generateComparator(RelCollations.of(0)));

            var rows = new List<object[]>
            {
                new object[] { Integer.valueOf(3), "c" },
                new object[] { Integer.valueOf(1), "a" },
                new object[] { Integer.valueOf(2), "b" },
            };

            rows.Sort(new Comparison<object[]>(compare));

            rows.ConvertAll(r => (string)r[1]).Should().Equal("a", "b", "c");
        }

        [Fact]
        public void ShouldTranslateAccessor()
        {
            var accessor = physType.generateAccessor(java.util.Arrays.asList([Integer.valueOf(0)]));
            var translated = AnonymousClasses.Unwrap(new LixToClrTranslator().Translate(accessor))!;

            var apply = translated.Compile();
            var key = apply.DynamicInvoke([new object[] { Integer.valueOf(7), "SMITH" }]);

            key.Should().NotBeNull();
        }

        [Fact]
        public void ShouldTranslateCollationKeyComparator()
        {
            // two collations, so the key is the whole row and the ordering lives entirely in the comparator
            var collations = java.util.Arrays.asList([
                new RelFieldCollation(0),
                new RelFieldCollation(1)]);

            var pair = physType.generateCollationKey(collations);

            // Pair's left and right fields are hidden in C# by static methods of the same names; Map.Entry's
            // getKey and getValue return the same two values
            pair.getValue().Should().NotBeNull("more than one collation puts the ordering in a comparator");

            var compare = Comparator((J.Expression)pair.getValue());

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(1), "b"]).Should().BeNegative();
        }

        [Fact]
        public void ShouldTranslateAnIdentitySelectorAsALinq4jFunction()
        {
            // where the projection is the identity, PhysType returns a call to Functions.identitySelector rather
            // than a lambda, so the translation is a linq4j Function1 and not a delegate
            var scalar = PhysTypeImpl.of(typeFactory, typeFactory.builder().add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)).build(), JavaRowFormat.ARRAY);

            var selector = scalar.generateSelector(
                J.Expressions.parameter(scalar.getJavaRowType(), "row"),
                java.util.Arrays.asList([Integer.valueOf(0)]),
                JavaRowFormat.ARRAY);

            var translated = new LixToClrTranslator().Translate(selector);

            translated.Should().NotBeAssignableTo<LambdaExpression>();
            translated.Type.Should().Be(typeof(org.apache.calcite.linq4j.function.Function1));
        }

        /// <summary>
        /// <see cref="ClrPhysTypeImpl"/> converts a cursor of one-column rows from <c>ARRAY</c> to
        /// <c>SCALAR</c>, as a one-value table function needs before it can be grouped.
        /// </summary>
        /// <remarks>
        /// <c>PhysTypeImpl</c>'s selector for this returns the primitive field type (<c>int</c>); a cursor of
        /// this convention carries the boxed type, so the converted cursor's element is <c>Integer</c>.
        /// </remarks>
        [Fact]
        public void ShouldConvertAOneColumnRowFromAnArrayToAScalar()
        {
            var oneColumn = typeFactory.builder().add("intField", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();
            var array = ClrPhysTypeImpl.Of(typeFactory, oneColumn, JavaRowFormat.ARRAY, false);

            var rows = new object[][] { [Integer.valueOf(1)], [Integer.valueOf(2)] };
            var source = Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorDefaults.AsCursor(rows);

            var converted = array.ConvertTo(Expression.Constant(source, typeof(IClrCursor<object[]>)), JavaRowFormat.SCALAR);

            converted.Type.Should().Be(typeof(IClrCursor<Integer>), "a cursor of a one-column scalar row carries the box");

            using var cursor = Expression.Lambda<Func<IClrCursor<Integer>>>(converted).Compile()();
            var value = new List<Integer>();
            while (cursor.Read())
                value.Add(cursor.Current);

            value.Should().Equal([Integer.valueOf(1), Integer.valueOf(2)]);
        }

    }

}
