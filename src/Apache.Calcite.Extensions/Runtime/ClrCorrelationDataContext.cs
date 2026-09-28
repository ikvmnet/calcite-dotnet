using System;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A <see cref="DataContext"/> that answers the named correlation variables with the rows it was given,
    /// and every other request from the context it wraps.
    /// </summary>
    /// <remarks>
    /// Used when a sub-plan of the cursor convention runs under Calcite's <c>EnumerableCorrelate</c>. The
    /// correlate's generated Java code holds the outer row as a lambda parameter, which a separately compiled
    /// sub-plan cannot see, so the converter passes each outer row in through this context as an
    /// <c>object[]</c>, and the sub-plan reads it with <see cref="get"/>.
    /// </remarks>
    public sealed class ClrCorrelationDataContext : DataContext
    {

        readonly DataContext parent;
        readonly string[] names;
        readonly object?[] rows;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parent">The context the plan was bound with.</param>
        /// <param name="names">The correlation variables' names.</param>
        /// <param name="rows">The outer rows, one per name, each an <c>object[]</c> of the row's fields.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="names"/> and <paramref name="rows"/> differ in
        /// length.</exception>
        public ClrCorrelationDataContext(DataContext parent, string[] names, object?[] rows)
        {
            this.parent = parent ?? throw new ArgumentNullException(nameof(parent));
            this.names = names ?? throw new ArgumentNullException(nameof(names));
            this.rows = rows ?? throw new ArgumentNullException(nameof(rows));

            if (names.Length != rows.Length)
                throw new ArgumentException("One row per name.", nameof(rows));
        }

        /// <inheritdoc />
        public SchemaPlus getRootSchema() => parent.getRootSchema();

        /// <inheritdoc />
        public JavaTypeFactory getTypeFactory() => parent.getTypeFactory();

        /// <inheritdoc />
        public QueryProvider getQueryProvider() => parent.getQueryProvider();

        /// <inheritdoc />
        public object get(string name)
        {
            for (int i = 0; i < names.Length; i++)
                if (names[i] == name)
                    return rows[i]!;

            return parent.get(name);
        }

    }

}
