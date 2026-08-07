using System;

using Apache.Calcite.Extensions.Prepare;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// The schema and context the prepare tests plan against.
    /// </summary>
    /// <remarks>
    /// Three tables, because the shapes differ where the port has broken: an <c>Object[]</c> row of several
    /// columns, an <c>Object[]</c> row of one — which <c>JavaRowFormat.optimize</c> turns into the value
    /// itself — and rows that are instances of a Java class, which take <c>PhysType</c>'s CUSTOM branch.
    /// </remarks>
    static class ClrPrepareFixture
    {

        /// <summary>
        /// Four columns, one nullable, so a null orders and groups.
        /// </summary>
        sealed class SalesTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(1), "EAST", java.lang.Integer.valueOf(10), "A"],
                [java.lang.Integer.valueOf(2), "EAST", java.lang.Integer.valueOf(20), "B"],
                [java.lang.Integer.valueOf(3), "EAST", java.lang.Integer.valueOf(20), "C"],
                [java.lang.Integer.valueOf(4), "WEST", java.lang.Integer.valueOf(30), "D"],
                [java.lang.Integer.valueOf(5), "WEST", null, "E"],
                [java.lang.Integer.valueOf(6), "NORTH", java.lang.Integer.valueOf(10), "A"],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("ID", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .add("REGION", typeFactory.createSqlType(SqlTypeName.VARCHAR, 10))
                    .add("AMOUNT", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true))
                    .add("GRADE", typeFactory.createSqlType(SqlTypeName.VARCHAR, 4))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) => Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// One column, which gives a scan a SCALAR physical type while the table still yields Object[].
        /// That shape has broken twice.
        /// </summary>
        sealed class OneColumnTable : AbstractTable, ScannableTable
        {

            static readonly object[][] Rows =
            [
                [java.lang.Integer.valueOf(3)],
                [java.lang.Integer.valueOf(1)],
                [java.lang.Integer.valueOf(2)],
            ];

            /// <inheritdoc />
            public override RelDataType getRowType(RelDataTypeFactory typeFactory)
            {
                return typeFactory.builder()
                    .add("N", typeFactory.createSqlType(SqlTypeName.INTEGER))
                    .build();
            }

            /// <inheritdoc />
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) => Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// Runs <paramref name="body"/> against a freshly built schema and context.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The statement, for the caller's own use.</param>
        /// <param name="body"></param>
        /// <returns></returns>
        /// <remarks>
        /// The context is pushed onto <c>CalcitePrepare.Dummy</c>'s thread-local stack for the length of the
        /// call, because Calcite's parse-to-rel reads it from there. A fresh schema per call, so one test
        /// cannot see another's plan cache.
        /// </remarks>
        public static T WithContext<T>(string sql, Func<CalcitePrepare.Context, CalciteSchema, T> body)
        {
            ArgumentNullException.ThrowIfNull(body);

            var rootSchema = CalciteSchema.createRootSchema(true);
            rootSchema.add("SALES", new SalesTable());
            rootSchema.add("NUMS", new OneColumnTable());
            rootSchema.plus().add("HR", new ReflectiveSchema(new org.apache.calcite.test.schemata.hr.HrSchema()));

            var typeFactory = new JavaTypeFactoryImpl();

            var properties = new java.util.Properties();
            properties.setProperty("lex", "JAVA");
            properties.setProperty("caseSensitive", "false");
            var config = new CalciteConnectionConfigImpl(properties);

            var context = new PrepareContext(typeFactory, rootSchema, config, []);

            CalcitePrepare.Dummy.push(context);
            try
            {
                return body(context, rootSchema);
            }
            finally
            {
                CalcitePrepare.Dummy.pop(context);
            }
        }

    }

}
