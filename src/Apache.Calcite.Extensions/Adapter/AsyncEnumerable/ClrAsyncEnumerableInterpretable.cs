using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Compiles a plan of the <see cref="ClrAsyncEnumerableConvention"/> calling convention into something that can
    /// be bound and run.
    /// </summary>
    /// <remarks>
    /// Holds what Calcite's <c>EnumerableInterpretable.toBindable</c> does: implement the plan, compile it,
    /// and hand back an <see cref="IClrAsyncBindable"/> — the counterpart of the <c>Bindable</c> Janino produces
    /// there.
    ///
    /// <para>Calcite declares that class as a converter node into <c>InterpretableConvention</c> as well,
    /// but nothing constructs one — the only <c>new EnumerableInterpretable</c> in 1.42 is inside its own
    /// <c>copy</c>, which needs an instance to exist already, and its constructor is protected. Its
    /// <c>implement</c> is unreachable, and <c>InterpretableConverter</c> is what actually wraps a subtree
    /// for the interpreter. So the class is a static holder there too, and this is not a partial port.</para>
    ///
    /// <para>A caller that wants the lambda itself rather than a compiled delegate casts the root to
    /// <see cref="ClrAsyncEnumerableRel"/> and uses
    /// <see cref="ClrAsyncEnumerableRelImplementor.ImplementRoot"/>.</para>
    /// </remarks>
    public static class ClrAsyncEnumerableInterpretable
    {

        /// <summary>
        /// Compiles a plan and returns it as an <see cref="IClrAsyncBindable"/>.
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
        /// <remarks>
        /// Calcite caches its compiled bindables — <c>EnumerableInterpretable.BINDABLE_CACHE</c>, keyed on
        /// the generated Java source text and skipped by <c>StaticFieldDetector</c> where the class holds a
        /// static field. There is nothing to port: a plan of this convention has no source text, so there is
        /// no key. A cache here would have to be built on the SQL and the schema's version instead, which is
        /// a different thing and belongs to whoever prepares rather than to whoever compiles.
        /// </remarks>
        public static IClrAsyncBindable ToBindable(java.util.Map internalParameters, org.apache.calcite.jdbc.CalcitePrepare.SparkHandler? spark, ClrAsyncEnumerableRel rel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(internalParameters);
            ArgumentNullException.ThrowIfNull(rel);

            if (spark != null && spark.enabled())
                throw new java.lang.UnsupportedOperationException(
                    "ClrAsyncEnumerableConvention cannot compile with a Spark handler: it has no generated class to hand one. Use EnumerableConvention for a Spark-enabled plan.");

            var implementor = new ClrAsyncEnumerableRelImplementor(rel.getCluster().getRexBuilder(), internalParameters);
            var lambda = implementor.ImplementRoot(rel, prefer);
            var plan = (Func<DataContext, IAsyncEnumerable<object>>)lambda.Compile();

            return new ClrAsyncBindable(plan, Linq4j.Tree.ClrTypes.Resolve(PhysTypeImpl.of(implementor.TypeFactory, rel.getRowType(), prefer.PreferArray()).getJavaRowType()));
        }

    }

}
