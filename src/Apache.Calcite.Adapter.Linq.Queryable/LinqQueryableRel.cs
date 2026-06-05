using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.Linq.Queryable
{

    public interface LinqQueryableRel : PhysicalNode
    {

        LinqQueryableRelResult Implement(LinqQueryableRelImplementor implementor);

    }

}
