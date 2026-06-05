using java.util;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.sql.validate;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;

namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    public class LinqEnumerableRelImplementor : RelImplementor
    {

        readonly Dictionary<string, object> _map = new();
        readonly Dictionary<string, RexToLixTranslator.InputGetter> _corrVars = new();

        /// <summary>
        /// Initializes a new instnace.
        /// </summary>
        public LinqEnumerableRelImplementor()
        {

        }

        public SqlConformance getConformance()
        {
            return SqlConformance.DEFAULT;
        }

        public LinqEnumerableRelResult VisitChild(LinqEnumerableRel parent, int ordinal, LinqEnumerableRel child)
        {
            if (parent != null)
                Debug.Assert(child == parent.getInputs().get(ordinal));

            return child.Implement(this);
        }

        public Func<IEnumerable> ImplementRoot(LinqEnumerableRel rootRel, LinqEnumerablePrefer prefer)
        {
            var result = rootRel.Implement(this);

            switch (prefer)
            {
                case LinqEnumerablePrefer.Array:
                    if (result.PhysType.getFormat() == JavaRowFormat.ARRAY && rootRel.getRowType().getFieldCount() == 1)
                    {

                    }

                    break;
                default:
                    break;
            }


        }

    }

}
