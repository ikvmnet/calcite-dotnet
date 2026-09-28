using System;

using Apache.Calcite.Geography.Sql;

using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Schema
{

    /// <summary>
    /// Declares the <c>CLR_ST_GEOG_*</c> operators as functions on a schema.
    /// </summary>
    /// <remarks>
    /// This is how the operators are made available without configuring the validator, for example through
    /// Calcite's JDBC driver or from an adapter that wants its functions to arrive with its tables. Functions added to
    /// the root schema are visible unqualified everywhere on the connection, because <c>CalciteCatalogReader</c>
    /// always searches the default schema and the root.
    ///
    /// <para>The functions are the same <c>ScalarFunctionImpl</c> objects <see cref="GeographyOperatorTable"/>
    /// holds, so an operator behaves the same whichever way it is reached. Use one route or the other, not both: a
    /// name found twice resolves to whichever the lookup reaches first. A call resolved through a schema lacks the
    /// strictness and symmetry the operator table declares until <c>GeographyRules</c> restores them.</para>
    /// </remarks>
    public static class GeographySchema
    {

        /// <summary>
        /// Adds every <c>CLR_ST_GEOG_*</c> operator to a schema as a function.
        /// </summary>
        /// <param name="schema">The schema to add to.</param>
        /// <returns><paramref name="schema"/>, for chaining.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="schema"/> is <c>null</c>.</exception>
        public static SchemaPlus AddTo(SchemaPlus schema)
        {
            ArgumentNullException.ThrowIfNull(schema);

            var operators = GeographyOperatorTable.Instance().getOperatorList();

            for (var i = 0; i < operators.size(); i++)
            {
                if (operators.get(i) is not SqlUserDefinedFunction function)
                    continue;

                schema.add(function.getName(), function.function);
            }

            return schema;
        }

    }

}
