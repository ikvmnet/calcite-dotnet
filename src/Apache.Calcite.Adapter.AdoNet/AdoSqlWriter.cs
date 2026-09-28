using org.apache.calcite.sql;
using org.apache.calcite.sql.pretty;

using System;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A <see cref="SqlPrettyWriter"/> that writes each dynamic parameter as the name the provider binds it
    /// under, from <see cref="IAdoSqlSyntax.GetParameterName"/>, and records which variable each one reads.
    /// </summary>
    /// <remarks>
    /// Calcite writes a dynamic parameter as a bare <c>?</c> because JDBC binds by position. Most ADO.NET providers
    /// match a <see cref="System.Data.Common.DbParameter"/> to the command text by name, so the name is written
    /// here as the statement is produced, where parameters cannot be confused with a <c>?</c> inside a string
    /// literal or a quoted identifier.
    /// </remarks>
    public class AdoSqlWriter : SqlPrettyWriter
    {

        readonly IAdoSqlSyntax _syntax;
        readonly java.util.ArrayList _indexes = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect">The dialect the SQL is written in.</param>
        /// <param name="syntax">Names each parameter.</param>
        /// <exception cref="ArgumentNullException"><paramref name="syntax"/> is <see langword="null"/>.</exception>
        public AdoSqlWriter(SqlDialect dialect, IAdoSqlSyntax syntax) :
            base(dialect)
        {
            _syntax = syntax ?? throw new ArgumentNullException(nameof(syntax));
        }

        /// <summary>
        /// Gets the variable index behind each parameter written so far, as <see cref="java.lang.Integer"/>s, in the
        /// order the parameters appear. Takes the place of <c>SqlString.getDynamicParameters</c>.
        /// </summary>
        public java.util.List Indexes => _indexes;

        /// <summary>
        /// Writes the parameter's name and records its variable index.
        /// </summary>
        /// <param name="index">The variable index the parameter reads.</param>
        public override void dynamicParam(int index)
        {
            // named by position among the parameters, which is how the enricher adds them
            print(_syntax.GetParameterName(_indexes.size()));
            _indexes.add(java.lang.Integer.valueOf(index));

            setNeedWhitespace(true);
        }

    }

}
