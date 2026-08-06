using org.apache.calcite.sql;
using org.apache.calcite.sql.pretty;

using System;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Writes SQL for an ADO.NET provider, naming each query parameter the way that provider binds it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite writes a dynamic parameter as a bare <c>?</c> and records its ordinal, because JDBC binds by
    /// position: <c>PreparedStatement.setObject(i, …)</c> never needs a name. ADO.NET has no positional
    /// binding — a <see cref="System.Data.Common.DbParameter"/> is matched to the command text by name — so
    /// a bare <c>?</c> is a marker nothing can fill.
    /// </para>
    /// <para>
    /// The marker is therefore written correctly the first time, rather than the text being patched
    /// afterwards. Patching would mean deciding which <c>?</c> in a finished statement is a parameter and
    /// which is data inside a string or a quoted identifier, and the writer already knows.
    /// </para>
    /// </remarks>
    public class AdoSqlWriter : SqlPrettyWriter
    {

        readonly IAdoSqlSyntax _syntax;
        readonly java.util.ArrayList _indexes = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect">The dialect the SQL is written for.</param>
        /// <param name="syntax">How the driver names a parameter.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public AdoSqlWriter(SqlDialect dialect, IAdoSqlSyntax syntax) :
            base(dialect)
        {
            _syntax = syntax ?? throw new ArgumentNullException(nameof(syntax));
        }

        /// <summary>
        /// Gets the variable index behind each parameter, in the order the parameters appear.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>SqlString.getDynamicParameters</c>, collected here because the marker is no
        /// longer the one the base writer records.
        /// </remarks>
        public java.util.List Indexes => _indexes;

        /// <inheritdoc />
        public override void dynamicParam(int index)
        {
            // the position among parameters, not the variable index, is what the name is derived from: it is
            // what the enricher counts by when it fills them
            print(_syntax.GetParameterName(_indexes.size()));
            _indexes.add(java.lang.Integer.valueOf(index));

            setNeedWhitespace(true);
        }

    }

}
