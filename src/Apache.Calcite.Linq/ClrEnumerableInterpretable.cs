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
    /// The counterpart of <c>EnumerableInterpretable</c>. This is the last step of running a query in this
    /// convention: give it the chosen plan and it returns a <see cref="Bindable"/> to bind to a
    /// <see cref="DataContext"/> and enumerate.
    /// </remarks>
    public static class ClrEnumerableInterpretable
    {

        /// <summary>
        /// Compiles a plan and returns it as a <see cref="Bindable"/>.
        /// </summary>
        /// <param name="internalParameters">Values the query reads through the <see cref="DataContext"/>
        /// rather than from the plan. The same map must be served by the context it is bound with.</param>
        /// <param name="spark">The Spark handler, or <see langword="null"/> where there is none. This
        /// convention cannot use one — see the exception.</param>
        /// <param name="rel">The chosen plan, whose root must be of this convention.</param>
        /// <param name="prefer">How the caller wants rows represented. <see cref="ClrEnumerablePrefer.Array"/>
        /// is what a prepared statement asks for.</param>
        /// <returns>The compiled plan, to bind to a <see cref="DataContext"/> and enumerate.</returns>
        /// <exception cref="java.lang.UnsupportedOperationException">
        /// <paramref name="spark"/> is enabled. A Spark handler compiles generated Java source, and a plan of
        /// this convention is an expression tree; use <c>EnumerableConvention</c> for such a query.
        /// </exception>
        /// <exception cref="java.lang.IllegalStateException">
        /// A node of the plan could not be implemented. The message names the plan.
        /// </exception>
        /// <remarks>
        /// Takes what <c>EnumerableInterpretable.toBindable</c> takes, in its order, so a caller holding one
        /// convention's compiler can hold the other's.
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
