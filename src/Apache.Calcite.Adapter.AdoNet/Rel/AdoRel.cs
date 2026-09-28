using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// A relational expression of an <see cref="AdoConvention"/>, which the <see cref="AdoImplementor"/> turns into
    /// SQL. The counterpart of Calcite's <c>JdbcRel</c>.
    /// </summary>
    public interface AdoRel : RelNode
    {

        /// <summary>
        /// Translates this node to SQL. The default hands the node to Calcite's <c>RelToSqlConverter</c> through
        /// <see cref="AdoImplementor.implement(AdoRel)"/>.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <returns>The SQL for this node and its inputs.</returns>
        public org.apache.calcite.rel.rel2sql.SqlImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.implement(this);
        }

    }

}
