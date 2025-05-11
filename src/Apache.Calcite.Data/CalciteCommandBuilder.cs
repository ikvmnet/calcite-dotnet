using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <inheritdoc />
    public class CalciteCommandBuilder : DbCommandBuilder
    {

        /// <inheritdoc />
        protected override void ApplyParameterInfo(DbParameter parameter, DataRow row, StatementType statementType, bool whereClause)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override string GetParameterName(int parameterOrdinal)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override string GetParameterName(string parameterName)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override string GetParameterPlaceholder(int parameterOrdinal)
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        protected override void SetRowUpdatingHandler(DbDataAdapter adapter)
        {
            throw new NotImplementedException();
        }

    }

}
