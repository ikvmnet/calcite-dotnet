using org.apache.calcite.plan;

namespace Apache.Calcite.Adapter.Linq.Queryable
{

    public class LinqQueryableConvention : Convention.Impl
    {

        public LinqQueryableConvention(string name) :
            base("LinqQueryable." + name, typeof(LinqQueryableRel))
        {

        }

    }

}
