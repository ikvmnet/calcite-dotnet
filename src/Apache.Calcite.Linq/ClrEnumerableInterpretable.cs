using System;
using System.Collections;

using Apache.Calcite.Linq.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// Compiles a plan of the <see cref="ClrEnumerableConvention"/> calling convention into something that can
    /// be bound and run.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableInterpretable</c>, which hands a generated class to Janino. Here the
    /// expression tree is compiled directly, and the result is wrapped as a <see cref="Bindable"/> so that
    /// everything downstream of a prepared statement is unchanged.
    /// </remarks>
    public static class ClrEnumerableInterpretable
    {

        /// <summary>
        /// Compiles a plan and returns it as a <see cref="Bindable"/>.
        /// </summary>
        /// <param name="internalParameters"></param>
        /// <param name="spark">The Spark handler, or <c>null</c> where there is none.</param>
        /// <param name="rel"></param>
        /// <param name="prefer"></param>
        /// <returns></returns>
        /// <remarks>
        /// The four arguments <c>EnumerableInterpretable.toBindable</c> takes, in its order, so that a caller
        /// holding one convention's compiler can hold the other's.
        ///
        /// <para><paramref name="spark"/> is taken and refused rather than dropped. Calcite hands the
        /// generated <c>ClassDeclaration</c> and its source text to <c>SparkHandler.compile</c> when one is
        /// enabled, and compiles with Janino otherwise. There is no generated class here and no source text —
        /// the plan is an expression tree — so there is nothing this convention could hand it. Dropping the
        /// parameter would have made a caller's Spark configuration silently ignored; refusing says which of
        /// the two conventions can honour it.</para>
        /// </remarks>
        public static Bindable ToBindable(java.util.Map internalParameters, org.apache.calcite.jdbc.CalcitePrepare.SparkHandler? spark, ClrEnumerableRel rel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(internalParameters);
            ArgumentNullException.ThrowIfNull(rel);

            if (spark != null && spark.enabled())
                throw new java.lang.UnsupportedOperationException(
                    "ClrEnumerableConvention cannot compile with a Spark handler: it has no generated class to hand one. Use EnumerableConvention for a Spark-enabled plan.");

            var implementor = new ClrEnumerableRelImplementor(rel.getCluster().getRexBuilder(), internalParameters);
            var lambda = implementor.ImplementRoot(rel, prefer);
            var plan = (Func<DataContext, IEnumerable>)lambda.Compile();

            return new ClrBindable(plan, PhysTypeImpl.of(implementor.TypeFactory, rel.getRowType(), prefer.PreferArray()).getJavaRowType());
        }

        /// <summary>
        /// A compiled plan, bound to a <see cref="DataContext"/> when it is run.
        /// </summary>
        /// <param name="plan"></param>
        /// <param name="elementType"></param>
        sealed class ClrBindable(Func<DataContext, IEnumerable> plan, java.lang.reflect.Type elementType) : Bindable, Typed
        {

            /// <inheritdoc />
            public Enumerable bind(DataContext dataContext)
            {
                return JavaSequences.ToJava(System.Linq.Enumerable.Cast<object>(plan(dataContext)));
            }

            /// <inheritdoc />
            public java.lang.reflect.Type getElementType()
            {
                return elementType;
            }

        }

    }

}
