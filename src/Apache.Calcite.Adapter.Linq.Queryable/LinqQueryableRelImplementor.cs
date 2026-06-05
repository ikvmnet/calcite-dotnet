using org.apache.calcite.plan;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Adapter.Linq.Queryable
{

    public class LinqQueryableRelImplementor : RelImplementor
    {

        public SqlConformance getConformance()
        {
            return SqlConformance.DEFAULT;
        }

    }

}
