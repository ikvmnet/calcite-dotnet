using System.Data;
using System.Data.Common;
using System.Data.Odbc;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for any <see cref="OdbcConnection"/>.
    /// </summary>
    class OdbcDatabaseMetadata : AdoInformationSchemaDatabaseMetadata<OdbcConnection>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource"></param>
        public OdbcDatabaseMetadata(DbDataSource dbDataSource) :
            base(dbDataSource)
        {

        }

        public override string GetDefaultSchema()
        {
            throw new System.NotImplementedException();
        }

        public override SqlDialect GetDialect()
        {
            throw new System.NotImplementedException();
        }

        protected override DbType ParseDbType(string typeName)
        {
            throw new System.NotImplementedException();
        }

    }

}
