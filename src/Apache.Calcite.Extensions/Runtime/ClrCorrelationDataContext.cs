using System;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// A <see cref="DataContext"/> that answers a correlation variable by name and everything else from the
    /// context it wraps.
    /// </summary>
    /// <remarks>
    /// What a sub-plan of either Clr convention reads its correlation variables through when it runs under a
    /// correlate of Calcite's. <c>EnumerableCorrelate</c> makes the outer row a parameter of the Java lambda
    /// it generates and the inner block reads it lexically; a sub-plan compiled apart from that lambda cannot
    /// see the parameter, so the converter hands the row in through the context instead — as a row of the
    /// <c>ARRAY</c> format whose fields Calcite's own getter read out of the parameter. The sub-plan's
    /// implementor registers the variable as a read of <see cref="get"/> and reads its fields from there.
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
