using System;

using Apache.Calcite.Geography.Sql;

using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Geography.Schema
{

    /// <summary>
    /// Puts the <c>ST_GEOG_*</c> operators into a schema, which is how a caller who is not driving Calcite's
    /// planner gets them.
    /// </summary>
    /// <remarks>
    /// A schema function is the only extension Calcite's JDBC driver supports without subclassing something.
    /// Registering into the root schema makes every operator visible unqualified to everything on the
    /// connection — <c>CalciteCatalogReader</c> always searches the default schema and the root, so a view in
    /// another schema resolves them too. An adapter that wants its functions to arrive with its tables calls
    /// this on the schema it is building, or a model file names the class and method of each.
    ///
    /// <para>This is what the operators being typed over <c>GEOMETRY</c> buys. A parameter type that Calcite
    /// cannot name — anything reaching <see cref="org.apache.calcite.sql.type.SqlTypeName.OTHER"/> — makes
    /// routine resolution throw <c>AssertionError: No assign rules for OTHER defined</c>, because
    /// <c>CalciteCatalogReader.toOp</c> builds a fixed-parameter checker for every schema function and there
    /// is no hook to supply one that would skip the comparison. Being ordinary geometries is what lets these
    /// go in a schema at all.</para>
    ///
    /// <para>The functions and <see cref="GeographyOperatorTable"/> are the same objects. Both hand out the
    /// <c>ScalarFunctionImpl</c> the operator table already built, so an operator behaves identically
    /// whichever way a host reaches it; a host driving its own planner may still chain the table instead, and
    /// should not do both, since a name found twice resolves to whichever the lookup reaches first.</para>
    /// </remarks>
    public static class GeographySchema
    {

        /// <summary>
        /// Registers every <c>ST_GEOG_*</c> operator on the given schema.
        /// </summary>
        /// <param name="schema"></param>
        /// <returns>The schema, for chaining.</returns>
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
