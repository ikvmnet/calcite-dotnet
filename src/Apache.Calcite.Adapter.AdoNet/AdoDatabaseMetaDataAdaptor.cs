using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using com.sun.org.apache.xml.@internal.utils;

using java.lang;
using java.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    class AdoDatabaseMetaDataAdaptor : DatabaseMetaData
    {

        readonly DbConnection _db;

        /// <summary>
        /// Exposes a GetSchema table from ADO.NET as a <see cref="DatabaseMetaData"/> interface.
        /// </summary>
        /// <param name="db"></param>
        public AdoDatabaseMetaDataAdaptor(DbConnection db)
        {
            _db = db ?? throw new ArgumentNullException(nameof(db));
        }

        public bool allProceduresAreCallable()
        {
            return true;
        }

        public bool allTablesAreSelectable()
        {
            return true;
        }

        public bool autoCommitFailureClosesAllResultSets()
        {
            throw new NotImplementedException();
        }

        public bool dataDefinitionCausesTransactionCommit()
        {
            throw new NotImplementedException();
        }

        public bool dataDefinitionIgnoredInTransactions()
        {
            throw new NotImplementedException();
        }

        public bool deletesAreDetected(int type)
        {
            throw new NotImplementedException();
        }

        public bool doesMaxRowSizeIncludeBlobs()
        {
            throw new NotImplementedException();
        }

        public bool generatedKeyAlwaysReturned()
        {
            throw new NotImplementedException();
        }

        public ResultSet getAttributes(string catalog, string schemaPattern, string typeNamePattern, string attributeNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getBestRowIdentifier(string catalog, string schema, string table, int scope, bool nullable)
        {
            throw new NotImplementedException();
        }

        public ResultSet getCatalogs()
        {
            throw new NotImplementedException();
        }

        public string getCatalogSeparator()
        {
            throw new NotImplementedException();
        }

        public string getCatalogTerm()
        {
            throw new NotImplementedException();
        }

        public ResultSet getClientInfoProperties()
        {
            throw new NotImplementedException();
        }

        public ResultSet getColumnPrivileges(string catalog, string schema, string table, string columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getColumns(string catalog, string schemaPattern, string tableNamePattern, string columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public Connection getConnection()
        {
            throw new NotImplementedException();
        }

        public ResultSet getCrossReference(string parentCatalog, string parentSchema, string parentTable, string foreignCatalog, string foreignSchema, string foreignTable)
        {
            throw new NotImplementedException();
        }

        public int getDatabaseMajorVersion()
        {
            throw new NotImplementedException();
        }

        public int getDatabaseMinorVersion()
        {
            throw new NotImplementedException();
        }

        public string getDatabaseProductName()
        {
            throw new NotImplementedException();
        }

        public string getDatabaseProductVersion()
        {
            throw new NotImplementedException();
        }

        public int getDefaultTransactionIsolation()
        {
            throw new NotImplementedException();
        }

        public int getDriverMajorVersion()
        {
            throw new NotImplementedException();
        }

        public int getDriverMinorVersion()
        {
            throw new NotImplementedException();
        }

        public string getDriverName()
        {
            throw new NotImplementedException();
        }

        public string getDriverVersion()
        {
            throw new NotImplementedException();
        }

        public ResultSet getExportedKeys(string catalog, string schema, string table)
        {
            throw new NotImplementedException();
        }

        public string getExtraNameCharacters()
        {
            throw new NotImplementedException();
        }

        public ResultSet getFunctionColumns(string catalog, string schemaPattern, string functionNamePattern, string columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getFunctions(string catalog, string schemaPattern, string functionNamePattern)
        {
            throw new NotImplementedException();
        }

        public string getIdentifierQuoteString()
        {
            throw new NotImplementedException();
        }

        public ResultSet getImportedKeys(string catalog, string schema, string table)
        {
            throw new NotImplementedException();
        }

        public ResultSet getIndexInfo(string catalog, string schema, string table, bool unique, bool approximate)
        {
            throw new NotImplementedException();
        }

        public int getJDBCMajorVersion()
        {
            throw new NotImplementedException();
        }

        public int getJDBCMinorVersion()
        {
            throw new NotImplementedException();
        }

        public int getMaxBinaryLiteralLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxCatalogNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxCharLiteralLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnsInGroupBy()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnsInIndex()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnsInOrderBy()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnsInSelect()
        {
            throw new NotImplementedException();
        }

        public int getMaxColumnsInTable()
        {
            throw new NotImplementedException();
        }

        public int getMaxConnections()
        {
            throw new NotImplementedException();
        }

        public int getMaxCursorNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxIndexLength()
        {
            throw new NotImplementedException();
        }

        public long getMaxLogicalLobSize()
        {
            throw new NotImplementedException();
        }

        public int getMaxProcedureNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxRowSize()
        {
            throw new NotImplementedException();
        }

        public int getMaxSchemaNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxStatementLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxStatements()
        {
            throw new NotImplementedException();
        }

        public int getMaxTableNameLength()
        {
            throw new NotImplementedException();
        }

        public int getMaxTablesInSelect()
        {
            throw new NotImplementedException();
        }

        public int getMaxUserNameLength()
        {
            throw new NotImplementedException();
        }

        public string getNumericFunctions()
        {
            throw new NotImplementedException();
        }

        public ResultSet getPrimaryKeys(string catalog, string schema, string table)
        {
            throw new NotImplementedException();
        }

        public ResultSet getProcedureColumns(string catalog, string schemaPattern, string procedureNamePattern, string columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getProcedures(string catalog, string schemaPattern, string procedureNamePattern)
        {
            throw new NotImplementedException();
        }

        public string getProcedureTerm()
        {
            throw new NotImplementedException();
        }

        public ResultSet getPseudoColumns(string catalog, string schemaPattern, string tableNamePattern, string columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public int getResultSetHoldability()
        {
            throw new NotImplementedException();
        }

        public RowIdLifetime getRowIdLifetime()
        {
            throw new NotImplementedException();
        }

        public ResultSet getSchemas()
        {
            throw new NotImplementedException();
        }

        public ResultSet getSchemas(string catalog, string schemaPattern)
        {
            throw new NotImplementedException();
        }

        public string getSchemaTerm()
        {
            throw new NotImplementedException();
        }

        public string getSearchStringEscape()
        {
            throw new NotImplementedException();
        }

        public string getSQLKeywords()
        {
            throw new NotImplementedException();
        }

        public int getSQLStateType()
        {
            throw new NotImplementedException();
        }

        public string getStringFunctions()
        {
            throw new NotImplementedException();
        }

        public ResultSet getSuperTables(string catalog, string schemaPattern, string tableNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getSuperTypes(string catalog, string schemaPattern, string typeNamePattern)
        {
            throw new NotImplementedException();
        }

        public string getSystemFunctions()
        {
            throw new NotImplementedException();
        }

        public ResultSet getTablePrivileges(string catalog, string schemaPattern, string tableNamePattern)
        {
            throw new NotImplementedException();
        }

        public ResultSet getTables(string catalog, string schemaPattern, string tableNamePattern, string[] types)
        {
            throw new NotImplementedException();
        }

        public ResultSet getTableTypes()
        {
            throw new NotImplementedException();
        }

        public string getTimeDateFunctions()
        {
            throw new NotImplementedException();
        }

        public ResultSet getTypeInfo()
        {
            throw new NotImplementedException();
        }

        public ResultSet getUDTs(string catalog, string schemaPattern, string typeNamePattern, int[] types)
        {
            throw new NotImplementedException();
        }

        public string getURL()
        {
            throw new NotImplementedException();
        }

        public string getUserName()
        {
            throw new NotImplementedException();
        }

        public ResultSet getVersionColumns(string catalog, string schema, string table)
        {
            throw new NotImplementedException();
        }

        public bool insertsAreDetected(int type)
        {
            throw new NotImplementedException();
        }

        public bool isCatalogAtStart()
        {
            throw new NotImplementedException();
        }

        public bool isReadOnly()
        {
            throw new NotImplementedException();
        }

        public bool isWrapperFor(Class iface)
        {
            throw new NotImplementedException();
        }

        public bool locatorsUpdateCopy()
        {
            throw new NotImplementedException();
        }

        public bool nullPlusNonNullIsNull()
        {
            throw new NotImplementedException();
        }

        public bool nullsAreSortedAtEnd()
        {
            throw new NotImplementedException();
        }

        public bool nullsAreSortedAtStart()
        {
            throw new NotImplementedException();
        }

        public bool nullsAreSortedHigh()
        {
            throw new NotImplementedException();
        }

        public bool nullsAreSortedLow()
        {
            throw new NotImplementedException();
        }

        public bool othersDeletesAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool othersInsertsAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool othersUpdatesAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool ownDeletesAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool ownInsertsAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool ownUpdatesAreVisible(int type)
        {
            throw new NotImplementedException();
        }

        public bool storesLowerCaseIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool storesLowerCaseQuotedIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool storesMixedCaseIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool storesMixedCaseQuotedIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool storesUpperCaseIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool storesUpperCaseQuotedIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool supportsAlterTableWithAddColumn()
        {
            throw new NotImplementedException();
        }

        public bool supportsAlterTableWithDropColumn()
        {
            throw new NotImplementedException();
        }

        public bool supportsANSI92EntryLevelSQL()
        {
            throw new NotImplementedException();
        }

        public bool supportsANSI92FullSQL()
        {
            throw new NotImplementedException();
        }

        public bool supportsANSI92IntermediateSQL()
        {
            throw new NotImplementedException();
        }

        public bool supportsBatchUpdates()
        {
            throw new NotImplementedException();
        }

        public bool supportsCatalogsInDataManipulation()
        {
            throw new NotImplementedException();
        }

        public bool supportsCatalogsInIndexDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsCatalogsInPrivilegeDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsCatalogsInProcedureCalls()
        {
            throw new NotImplementedException();
        }

        public bool supportsCatalogsInTableDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsColumnAliasing()
        {
            throw new NotImplementedException();
        }

        public bool supportsConvert()
        {
            throw new NotImplementedException();
        }

        public bool supportsConvert(int fromType, int toType)
        {
            throw new NotImplementedException();
        }

        public bool supportsCoreSQLGrammar()
        {
            throw new NotImplementedException();
        }

        public bool supportsCorrelatedSubqueries()
        {
            throw new NotImplementedException();
        }

        public bool supportsDataDefinitionAndDataManipulationTransactions()
        {
            throw new NotImplementedException();
        }

        public bool supportsDataManipulationTransactionsOnly()
        {
            throw new NotImplementedException();
        }

        public bool supportsDifferentTableCorrelationNames()
        {
            throw new NotImplementedException();
        }

        public bool supportsExpressionsInOrderBy()
        {
            throw new NotImplementedException();
        }

        public bool supportsExtendedSQLGrammar()
        {
            throw new NotImplementedException();
        }

        public bool supportsFullOuterJoins()
        {
            throw new NotImplementedException();
        }

        public bool supportsGetGeneratedKeys()
        {
            throw new NotImplementedException();
        }

        public bool supportsGroupBy()
        {
            throw new NotImplementedException();
        }

        public bool supportsGroupByBeyondSelect()
        {
            throw new NotImplementedException();
        }

        public bool supportsGroupByUnrelated()
        {
            throw new NotImplementedException();
        }

        public bool supportsIntegrityEnhancementFacility()
        {
            throw new NotImplementedException();
        }

        public bool supportsLikeEscapeClause()
        {
            throw new NotImplementedException();
        }

        public bool supportsLimitedOuterJoins()
        {
            throw new NotImplementedException();
        }

        public bool supportsMinimumSQLGrammar()
        {
            throw new NotImplementedException();
        }

        public bool supportsMixedCaseIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool supportsMixedCaseQuotedIdentifiers()
        {
            throw new NotImplementedException();
        }

        public bool supportsMultipleOpenResults()
        {
            throw new NotImplementedException();
        }

        public bool supportsMultipleResultSets()
        {
            throw new NotImplementedException();
        }

        public bool supportsMultipleTransactions()
        {
            throw new NotImplementedException();
        }

        public bool supportsNamedParameters()
        {
            throw new NotImplementedException();
        }

        public bool supportsNonNullableColumns()
        {
            throw new NotImplementedException();
        }

        public bool supportsOpenCursorsAcrossCommit()
        {
            throw new NotImplementedException();
        }

        public bool supportsOpenCursorsAcrossRollback()
        {
            throw new NotImplementedException();
        }

        public bool supportsOpenStatementsAcrossCommit()
        {
            throw new NotImplementedException();
        }

        public bool supportsOpenStatementsAcrossRollback()
        {
            throw new NotImplementedException();
        }

        public bool supportsOrderByUnrelated()
        {
            throw new NotImplementedException();
        }

        public bool supportsOuterJoins()
        {
            throw new NotImplementedException();
        }

        public bool supportsPositionedDelete()
        {
            throw new NotImplementedException();
        }

        public bool supportsPositionedUpdate()
        {
            throw new NotImplementedException();
        }

        public bool supportsRefCursors()
        {
            throw new NotImplementedException();
        }

        public bool supportsResultSetConcurrency(int type, int concurrency)
        {
            throw new NotImplementedException();
        }

        public bool supportsResultSetHoldability(int holdability)
        {
            throw new NotImplementedException();
        }

        public bool supportsResultSetType(int type)
        {
            throw new NotImplementedException();
        }

        public bool supportsSavepoints()
        {
            throw new NotImplementedException();
        }

        public bool supportsSchemasInDataManipulation()
        {
            throw new NotImplementedException();
        }

        public bool supportsSchemasInIndexDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsSchemasInPrivilegeDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsSchemasInProcedureCalls()
        {
            throw new NotImplementedException();
        }

        public bool supportsSchemasInTableDefinitions()
        {
            throw new NotImplementedException();
        }

        public bool supportsSelectForUpdate()
        {
            throw new NotImplementedException();
        }

        public bool supportsStatementPooling()
        {
            throw new NotImplementedException();
        }

        public bool supportsStoredFunctionsUsingCallSyntax()
        {
            throw new NotImplementedException();
        }

        public bool supportsStoredProcedures()
        {
            throw new NotImplementedException();
        }

        public bool supportsSubqueriesInComparisons()
        {
            throw new NotImplementedException();
        }

        public bool supportsSubqueriesInExists()
        {
            throw new NotImplementedException();
        }

        public bool supportsSubqueriesInIns()
        {
            throw new NotImplementedException();
        }

        public bool supportsSubqueriesInQuantifieds()
        {
            throw new NotImplementedException();
        }

        public bool supportsTableCorrelationNames()
        {
            throw new NotImplementedException();
        }

        public bool supportsTransactionIsolationLevel(int level)
        {
            throw new NotImplementedException();
        }

        public bool supportsTransactions()
        {
            throw new NotImplementedException();
        }

        public bool supportsUnion()
        {
            throw new NotImplementedException();
        }

        public bool supportsUnionAll()
        {
            throw new NotImplementedException();
        }

        public object unwrap(Class iface)
        {
            throw new NotImplementedException();
        }

        public bool updatesAreDetected(int type)
        {
            throw new NotImplementedException();
        }

        public bool usesLocalFilePerTable()
        {
            throw new NotImplementedException();
        }

        public bool usesLocalFiles()
        {
            throw new NotImplementedException();
        }
    }

}
