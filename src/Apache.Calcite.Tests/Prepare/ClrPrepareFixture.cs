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

namespace Apache.Calcite.Extensions.Prepare.Tests
{

    /// <summary>
    /// The schema and context the prepare tests plan against.
    /// </summary>
    /// <remarks>
    /// The tables cover three row shapes: an <c>Object[]</c> of several columns (<c>SALES</c>), an
    /// <c>Object[]</c> of one column, which <c>JavaRowFormat.optimize</c> turns into the value itself
    /// (<c>NUMS</c>), and instances of a Java class, which take <c>PhysType</c>'s <c>CUSTOM</c> branch
    /// (<c>HR</c>).
    /// </remarks>
    static class ClrPrepareFixture
    {

        /// <summary>
        /// Four columns, one nullable, so a null orders and groups.
        /// </summary>
        sealed class SalesTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
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
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) => org.apache.calcite.linq4j.Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// One column, which gives a scan a <c>SCALAR</c> physical type while the table yields <c>Object[]</c>.
        /// </summary>
        sealed class OneColumnTable : AbstractTable, ScannableTable
        {

            static readonly object?[][] Rows =
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
            public org.apache.calcite.linq4j.Enumerable scan(DataContext root) => org.apache.calcite.linq4j.Linq4j.asEnumerable(Rows);

        }

        /// <summary>
        /// Runs <paramref name="body"/> against a newly built schema and prepare context.
        /// </summary>
        /// <typeparam name="T">The type <paramref name="body"/> returns.</typeparam>
        /// <param name="sql">The statement; unused here, and available to the caller.</param>
        /// <param name="body">The test, given the context and the root schema.</param>
        /// <param name="connectionProperties">Applied to the connection properties after the defaults, for a
        /// test of a connection property.</param>
        /// <returns>What <paramref name="body"/> returns.</returns>
        /// <remarks>
        /// The context is pushed onto <c>CalcitePrepare.Dummy</c>'s thread-local stack for the duration of the
        /// call, because Calcite's parse-to-rel reads it from there. Each call builds its own schema, so tests
        /// share no cached state.
        /// </remarks>
        public static T WithContext<T>(string sql, Func<CalcitePrepare.Context, CalciteSchema, T> body, Action<java.util.Properties>? connectionProperties = null)
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
            connectionProperties?.Invoke(properties);
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
