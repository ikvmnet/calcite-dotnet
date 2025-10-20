using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.AdoNet.Rel
{

    /// <summary>
    /// Relational expression that uses ADO.NET calling convention.
    /// </summary>
    public interface AdoRel : RelNode
    {

        /// <summary>
        /// Invoked by the <see cref="AdoImplementor"/>.
        /// </summary>
        /// <param name="implementor"></param>
        /// <returns></returns>
        public org.apache.calcite.rel.rel2sql.SqlImplementor.Result implement(AdoImplementor implementor)
        {
            return implementor.implement(this);
        }

    }

}
