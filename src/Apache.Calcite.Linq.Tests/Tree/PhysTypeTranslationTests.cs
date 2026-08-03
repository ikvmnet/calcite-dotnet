using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Tree;

using FluentAssertions;

using java.lang;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tests.Tree
{

    /// <summary>
    /// Translates what <see cref="PhysTypeImpl"/> actually emits.
    /// </summary>
    /// <remarks>
    /// The port reuses every expression-producing member of <c>PhysType</c> rather than writing CLR versions
    /// of them, which is only worth anything if what they emit survives translation. These run the result.
    /// </remarks>
    [TestClass]
    public class PhysTypeTranslationTests
    {

        JavaTypeFactoryImpl typeFactory = null!;
        RelDataType rowType = null!;
        PhysType physType = null!;

        [TestInitialize]
        public void Setup()
        {
            typeFactory = new JavaTypeFactoryImpl();
            rowType = typeFactory.builder()
                .add("EMPNO", typeFactory.createSqlType(SqlTypeName.INTEGER))
                .add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR))
                .build();
            physType = PhysTypeImpl.of(typeFactory, rowType, JavaRowFormat.ARRAY);
        }

        /// <summary>
        /// Translates a comparator and returns it as something that can be called.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static Func<object[], object[], int> Comparator(J.Expression expression)
        {
            var translated = new ExpressionTranslator().Translate(expression);

            translated.Type.Should().Be(typeof(java.util.Comparator), "an anonymous class is wrapped back into its interface");

            var comparator = Expression.Lambda<Func<java.util.Comparator>>(translated).Compile()();

            return (x, y) => comparator.compare(x, y);
        }

        [TestMethod]
        public void ShouldTranslateFieldReference()
        {
            var row = J.Expressions.parameter(physType.getJavaRowType(), "row");
            var target = Expression.Parameter(typeof(object[]), "row");

            var translator = new ExpressionTranslator();
            translator.Bind(row, target);

            var name = translator.Translate(physType.fieldReference(row, 1));
            var read = Expression.Lambda<Func<object[], object>>(Expression.Convert(name, typeof(object)), target).Compile();

            read([Integer.valueOf(7), "SMITH"]).Should().Be("SMITH");
        }

        [TestMethod]
        public void ShouldTranslateRecord()
        {
            var translator = new ExpressionTranslator();
            var record = translator.Translate(
                physType.record(java.util.Arrays.asList([
                    J.Expressions.constant(Integer.valueOf(7)),
                    J.Expressions.constant("SMITH")])));

            var build = Expression.Lambda<Func<object[]>>(record).Compile();

            build().Should().BeEquivalentTo(new object[] { Integer.valueOf(7), "SMITH" });
        }

        [TestMethod]
        public void ShouldTranslateSelector()
        {
            var row = J.Expressions.parameter(physType.getJavaRowType(), "row");
            var selector = physType.generateSelector(row, java.util.Arrays.asList([Integer.valueOf(1)]));

            var translated = SamAdapters.Unwrap(new ExpressionTranslator().Translate(selector))!;
            var apply = translated.Compile();

            apply.DynamicInvoke([new object[] { Integer.valueOf(7), "SMITH" }]).Should().Be("SMITH");
        }

        [TestMethod]
        public void ShouldTranslateComparator()
        {
            // an anonymous java.util.Comparator, which an expression tree cannot declare, so it becomes the
            // lambda its compare method already is, wrapped back into the interface it was declared against
            var collation = RelCollations.of(0);
            var compare = Comparator(physType.generateComparator(collation));

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(2), "b"]).Should().BeNegative();
            compare([Integer.valueOf(2), "b"], [Integer.valueOf(1), "a"]).Should().BePositive();
            compare([Integer.valueOf(1), "a"], [Integer.valueOf(1), "z"]).Should().Be(0);
        }

        [TestMethod]
        public void ShouldTranslateDescendingComparator()
        {
            var collation = RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING));
            var compare = Comparator(physType.generateComparator(collation));

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(2), "b"]).Should().BePositive();
        }

        [TestMethod]
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

        [TestMethod]
        public void ShouldTranslateAccessor()
        {
            var accessor = physType.generateAccessor(java.util.Arrays.asList([Integer.valueOf(0)]));
            var translated = SamAdapters.Unwrap(new ExpressionTranslator().Translate(accessor))!;

            var apply = translated.Compile();
            var key = apply.DynamicInvoke([new object[] { Integer.valueOf(7), "SMITH" }]);

            key.Should().NotBeNull();
        }

        [TestMethod]
        public void ShouldTranslateCollationKeyComparator()
        {
            // two collations, so the key is the whole row and the ordering lives entirely in the comparator
            var collations = java.util.Arrays.asList([
                new RelFieldCollation(0),
                new RelFieldCollation(1)]);

            var pair = physType.generateCollationKey(collations);

            // Pair exposes left and right as fields, and also declares static methods of those names, which
            // C# cannot tell apart. Map.Entry gives the same two values under names that are not overloaded.
            pair.getValue().Should().NotBeNull("more than one collation puts the ordering in a comparator");

            var compare = Comparator((J.Expression)pair.getValue());

            compare([Integer.valueOf(1), "a"], [Integer.valueOf(1), "b"]).Should().BeNegative();
        }

        [TestMethod]
        public void ShouldTranslateAnIdentitySelectorAsALinq4jFunction()
        {
            // PhysType does not always return a lambda. Where the projection is the identity it returns a call
            // to Functions.identitySelector, whose value is a linq4j Function1 rather than anything a delegate
            // can be made from, and a node asking for a selector has to be ready for either.
            var scalar = PhysTypeImpl.of(typeFactory, typeFactory.builder().add("NAME", typeFactory.createSqlType(SqlTypeName.VARCHAR)).build(), JavaRowFormat.ARRAY);

            var selector = scalar.generateSelector(
                J.Expressions.parameter(scalar.getJavaRowType(), "row"),
                java.util.Arrays.asList([Integer.valueOf(0)]),
                JavaRowFormat.ARRAY);

            var translated = new ExpressionTranslator().Translate(selector);

            translated.Should().NotBeAssignableTo<LambdaExpression>();
            translated.Type.Should().Be(typeof(org.apache.calcite.linq4j.function.Function1));
        }

    }

}
