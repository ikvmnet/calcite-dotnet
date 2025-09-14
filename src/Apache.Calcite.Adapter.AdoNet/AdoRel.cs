using org.apache.calcite.rel;

namespace Apache.Calcite.Adapter.AdoNet
{

    public interface AdoRel : RelNode
    {

        AdoImplementor.Result implement(AdoImplementor implementor);

    }

}
