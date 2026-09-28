using System.Linq;

using Apache.Calcite.Extensions.Interop;

using FluentAssertions;

using IKVM.Runtime;

using java.lang;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.sql.type;

using Xunit;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Interop.Tests
{

    /// <summary>
    /// <c>EnumerableMatch.PassedRowsInputGetter</c> and <c>PrevInputGetter</c> are package private types
    /// <c>RexToLixTranslator</c> and <c>RexImpTable</c> cast to by name, so a class of the same shape will not
    /// do. IKVM compiles them <c>internal</c>, and C# cannot call <c>new</c> on them; a delegate over the
    /// constructor can, and what it returns is Calcite's own class.
    /// </summary>
    public class EnumerableMatchInputGetterTests
    {

        static Class Nested(string name) =>
            ((Class)typeof(EnumerableMatch)).getDeclaredClasses().Single(c => c.getSimpleName() == name);

        static java.lang.reflect.Constructor Constructor(Class clazz)
        {
            var c = clazz.getDeclaredConstructors().Single();
            c.setAccessible(true);
            return c;
        }

        static PhysType PhysType()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var rowType = typeFactory.builder().add("A", SqlTypeName.INTEGER).add("B", SqlTypeName.VARCHAR).build();
            return PhysTypeImpl.of(typeFactory, rowType, JavaRowFormat.ARRAY);
        }

        [Fact]
        public void ShouldConstructAPrevInputGetter()
        {
            var clazz = Nested("PrevInputGetter");
            var row = J.Expressions.parameter((Class)typeof(object[]), "row_");

            var getter = ((MH<object, object, object>)JavaDelegates.FromMethod(Constructor(clazz)))(row, PhysType());
            clazz.isInstance(getter).Should().BeTrue();

            var setOffset = clazz.getDeclaredMethod("setOffset", (Class)typeof(J.Expression));
            setOffset.setAccessible(true);
            ((MHV<object, object>)JavaDelegates.FromMethod(setOffset))(getter, J.Expressions.constant(Integer.valueOf(1)));

            ((RexToLixTranslator.InputGetter)getter).field(new J.BlockBuilder(), 0, null).Should().NotBeNull();
        }

        [Fact]
        public void ShouldConstructAPassedRowsInputGetter()
        {
            var clazz = Nested("PassedRowsInputGetter");
            var row = J.Expressions.parameter((Class)typeof(object[]), "row_");
            var rows = J.Expressions.parameter((Class)typeof(java.util.List), "rows_");

            var getter = ((MH<object, object, object, object>)JavaDelegates.FromMethod(Constructor(clazz)))(row, rows, PhysType());
            clazz.isInstance(getter).Should().BeTrue();

            ((RexToLixTranslator.InputGetter)getter).field(new J.BlockBuilder(), 0, null).Should().NotBeNull();
        }

    }

}
