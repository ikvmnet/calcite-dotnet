using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Relational expression that uses ADO.NET calling convention.
    /// </summary>
    public interface AdoRel : RelNode
    {

        AdoImplementor.Result implement(AdoImplementor implementor);

    }

}
