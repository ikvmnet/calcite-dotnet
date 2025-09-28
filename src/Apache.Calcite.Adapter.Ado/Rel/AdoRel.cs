using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.Ado.Rel
{

    /// <summary>
    /// Relational expression that uses ADO.NET calling convention.
    /// </summary>
    interface AdoRel : RelNode
    {

        org.apache.calcite.rel.rel2sql.SqlImplementor.Result implement(AdoImplementor implementor);

    }

}
