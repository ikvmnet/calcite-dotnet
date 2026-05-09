using System;
using System.Data;
using System.Linq;

using Apache.Calcite.Data.Internal;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteConnectionGetSchemaTests
    {

        [Fact]
        public void GetSchema_with_no_args_should_return_metadata_collections()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema();

            Assert.Equal(CalciteSchemaInfo.MetaDataCollections, t.TableName);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["CollectionName"]).ToArray();
            Assert.Contains(CalciteSchemaInfo.MetaDataCollections, names);
            Assert.Contains(CalciteSchemaInfo.Restrictions, names);
            Assert.Contains(CalciteSchemaInfo.DataSourceInformation, names);
            Assert.Contains(CalciteSchemaInfo.DataTypes, names);
            Assert.Contains(CalciteSchemaInfo.ReservedWords, names);
        }

        [Fact]
        public void GetSchema_should_return_restrictions()
        {
            using var c = new CalciteConnection();
            var t = c.GetSchema(CalciteSchemaInfo.Restrictions);

            Assert.Equal(CalciteSchemaInfo.Restrictions, t.TableName);
            Assert.Contains("RestrictionName", t.Columns.Cast<DataColumn>().Select(col => col.ColumnName));
            Assert.Contains("ParameterName", t.Columns.Cast<DataColumn>().Select(col => col.ColumnName));
        }

        [Fact]
        public void GetSchema_should_return_data_source_information()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.DataSourceInformation);
            Assert.Single(t.Rows);
            var row = t.Rows[0];
            Assert.Equal("Apache Calcite", row["DataSourceProductName"]);
            Assert.Equal("?", row["ParameterMarkerFormat"]);
        }

        [Fact]
        public void GetSchema_should_return_data_types()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.DataTypes);
            var names = t.Rows.Cast<DataRow>().Select(r => (string)r["TypeName"]).ToArray();
            Assert.Contains("INTEGER", names);
            Assert.Contains("VARCHAR", names);
            Assert.Contains("TIMESTAMP", names);
            Assert.Contains("DECIMAL", names);
        }

        [Fact]
        public void GetSchema_should_return_reserved_words()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            var t = c.GetSchema(CalciteSchemaInfo.ReservedWords);
            var words = t.Rows.Cast<DataRow>().Select(r => (string)r["ReservedWord"]).ToArray();
            Assert.Contains("SELECT", words);
            Assert.Contains("FROM", words);
            Assert.Contains("WHERE", words);
        }

        [Fact]
        public void GetSchema_should_throw_for_unknown_collection()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Throws<ArgumentException>(() => c.GetSchema("not_a_real_collection"));
        }

        [Fact]
        public void GetSchema_should_throw_when_collection_name_is_null()
        {
            using var c = new CalciteConnection();
            Assert.Throws<ArgumentNullException>(() => c.GetSchema(null!));
        }

    }

}
