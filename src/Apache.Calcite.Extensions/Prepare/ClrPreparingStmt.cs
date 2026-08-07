using System;

using org.apache.calcite.adapter.java;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.validate;
using org.apache.calcite.sql2rel;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A statement being prepared against a Calcite schema.
    /// </summary>
    /// <remarks>
    /// Our counterpart of <c>CalcitePrepareImpl.CalcitePreparingStmt</c>: the wiring half, holding the
    /// cluster, the planner, the convertlet table and the schema, and answering everything
    /// <see cref="ClrPrepare"/> leaves abstract. It is also the view expander a
    /// <c>SqlToRelConverter</c> is given.
    ///
    /// <para>Three things are still a convention's own and nothing else is: the convention a plan must end
    /// in, the program that gets it there, and the compiler at the end. A subclass supplies those, and that
    /// is the whole of what a second convention writes.</para>
    /// </remarks>
    abstract class ClrPreparingStmt : ClrPrepare, RelOptTable.ViewExpander
    {

        readonly CalciteSchema schema;
        readonly RelOptCluster cluster;
        readonly RexBuilder rexBuilder;
        readonly JavaTypeFactory typeFactory;
        readonly SqlRexConvertletTable convertletTable;

        /// <summary>
        /// The values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        /// <remarks>
        /// Ours rather than Calcite's. <c>CalcitePreparingStmt.internalParameters</c> is private and written
        /// only inside the <c>implement</c> a subclass replaces, so a port that derived from it stashed into
        /// one map and served the query from another.
        /// </remarks>
        readonly java.util.Map internalParameters = new java.util.LinkedHashMap();

        SqlValidator? validator;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        protected ClrPreparingStmt(
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            CalciteSchema schema,
            RelOptCluster cluster,
            Convention resultConvention,
            SqlRexConvertletTable convertletTable) :
            base(context, catalogReader, resultConvention)
        {
            this.schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
            this.convertletTable = convertletTable ?? throw new ArgumentNullException(nameof(convertletTable));

            rexBuilder = cluster.getRexBuilder();
            typeFactory = (JavaTypeFactory)cluster.getTypeFactory();
        }

        /// <summary>
        /// Gets the cluster the plan is built in.
        /// </summary>
        protected RelOptCluster Cluster => cluster;

        /// <summary>
        /// Gets the values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        public java.util.Map InternalParameters => internalParameters;

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.getSqlValidator</c>, which caches so that one statement cannot end up
        /// with two.
        /// </remarks>
        protected override SqlValidator Validator => validator ??= CreateSqlValidator(CatalogReader, c => c);

        /// <summary>
        /// Gets the validator the statement was validated with, for the driver.
        /// </summary>
        /// <remarks>
        /// Calcite's driver reads <c>getSqlValidator</c> even though it is protected, because
        /// <c>CalcitePreparingStmt</c> is a nested class of <c>CalcitePrepareImpl</c> and Java lets an outer
        /// class reach a nested one's protected members. C# has no equivalent, so the accessor is public
        /// rather than the member's protection being widened.
        /// </remarks>
        public SqlValidator SqlValidator => Validator;

        /// <summary>
        /// Prepares a plan that was built rather than parsed.
        /// </summary>
        /// <param name="rel">The plan to run.</param>
        /// <param name="resultType">The row type a caller wants the result described as.</param>
        /// <returns>The compiled statement.</returns>
        /// <remarks>
        /// <c>CalcitePreparingStmt.prepare_(Supplier&lt;RelNode&gt;, RelDataType)</c>, which is how Calcite
        /// answers <c>RelRunner</c> and a linq4j <c>Queryable</c>. It is a shorter pipeline than
        /// <c>PrepareSql</c> and says so: there is nothing to validate, so there are no dynamic parameters
        /// and no field origins, and no <c>EXPLAIN</c>, checked-arithmetic or decorrelation pass.
        ///
        /// <para>Calcite reaches this through <c>Query.of(rel)</c> and <c>Query.of(queryable)</c>. The
        /// queryable half cannot be ported — <c>LixToRelTranslator</c> is package-private, takes a
        /// <c>Prepare</c>, and translates linq4j expression trees, which is Java's LINQ rather than
        /// .NET's — so this is where a .NET provider's own translation would land instead.</para>
        /// </remarks>
        public ClrPrepareResult PrepareRel(RelNode rel, RelDataType resultType)
        {
            ArgumentNullException.ThrowIfNull(rel);
            ArgumentNullException.ThrowIfNull(resultType);

            var rowType = rel.getRowType();
            var fields = org.apache.calcite.util.Pair.zip(
                org.apache.calcite.util.ImmutableIntList.identity(rowType.getFieldCount()),
                rowType.getFieldNames());

            var collation = rel is org.apache.calcite.rel.core.Sort sort
                ? sort.collation
                : RelCollations.EMPTY;

            var root = new RelRoot(rel, resultType, SqlKind.SELECT, fields, collation, com.google.common.collect.ImmutableList.of());

            // no validation happened, so there is nothing to say about where a field came from or what
            // parameters the statement takes — which is what Calcite records here too
            var jdbcType = MakeStruct(rexBuilder.getTypeFactory(), resultType);
            FieldOrigins = java.util.Collections.nCopies(jdbcType.getFieldCount(), null);
            ParameterRowType = rexBuilder.getTypeFactory().builder().build();

            root = root.withRel(FlattenTypes(root.rel, true));
            root = TrimUnusedFields(root);
            root = Optimize(root);

            org.apache.calcite.runtime.Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);

            return Implement(root);
        }

        /// <summary>
        /// Wraps a type in a one-field struct where it is not one already.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.makeStruct</c>, a private static there.
        /// </remarks>
        static RelDataType MakeStruct(RelDataTypeFactory typeFactory, RelDataType type)
        {
            return type.isStruct() ? type : typeFactory.builder().add("$0", type).build();
        }

        /// <inheritdoc />
        /// <remarks>
        /// Empty, where Calcite queries the <c>MaterializationService</c> and populates each through
        /// <c>CalcitePrepareImpl.populateMaterializations</c> — which builds a <c>CalciteMaterializer</c>, a
        /// package-private class of <c>org.apache.calcite.prepare</c> with a package-private constructor.
        /// It cannot be reached and cannot be written again, so a materialized view is not substituted here
        /// however the connection is configured.
        /// </remarks>
        protected override java.util.List GetMaterializations()
        {
            return com.google.common.collect.ImmutableList.of();
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>Schemas.getLatticeEntries</c>, as Calcite's does. A lattice needs a star table to be useful and
        /// <see cref="GetMaterializations"/> supplies none, so this is empty in practice; it is read rather
        /// than assumed empty because that is a fact about the schema rather than about this class.
        /// </remarks>
        protected override java.util.List GetLattices()
        {
            var entries = org.apache.calcite.schema.Schemas.getLatticeEntries(schema);
            var lattices = new java.util.ArrayList();

            for (var i = entries.iterator(); i.hasNext();)
            {
                var lattice = (CalciteSchema.LatticeEntry)i.next();
                var starTable = lattice.getStarTable();
                var starRelOptTable = RelOptTableImpl.create(CatalogReader, starTable.getTable().getRowType(typeFactory), starTable, null);
                lattices.add(new RelOptLattice(lattice.getLattice(), starRelOptTable));
            }

            return lattices;
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.flattenTypes</c>, which flattens only under a Spark handler and otherwise
        /// hands the plan back. This convention cannot use a Spark handler at all — it has no generated
        /// class to give one — so this is the branch Calcite takes without one.
        /// </remarks>
        protected override RelNode FlattenTypes(RelNode rootRel, bool restructure)
        {
            return rootRel;
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.decorrelate</c>. Reached only when the connection asks for it: a
        /// correlate is a node these conventions implement, and decorrelating removes the node they would
        /// have used.
        /// </remarks>
        protected override RelNode Decorrelate(SqlToRelConverter sqlToRelConverter, SqlNode query, RelNode rootRel)
        {
            return sqlToRelConverter.decorrelate(query, rootRel);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.getSqlToRelConverter</c>. This class is the view expander it is given.
        /// </remarks>
        protected override SqlToRelConverter GetSqlToRelConverter(SqlValidator validator, org.apache.calcite.prepare.Prepare.CatalogReader catalogReader, SqlToRelConverter.Config config)
        {
            config = config.withTopDownGeneralDecorrelationEnabled(Context.config().topDownGeneralDecorrelationEnabled());

            return new SqlToRelConverter(this, validator, catalogReader, cluster, convertletTable, config);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>Prepare.PreparedExplain</c>'s <c>getCode</c>, which is where the text is produced rather than
        /// at the point the statement is run.
        /// </remarks>
        protected override ClrPrepareResult CreatePreparedExplanation(
            RelDataType? resultType,
            RelDataType parameterRowType,
            RelRoot? root,
            SqlExplainFormat format,
            SqlExplainLevel detailLevel)
        {
            var explanation = root == null
                ? RelOptUtil.dumpType(requireResultType(resultType))
                : RelOptUtil.dumpPlan("", root.rel, format, detailLevel);

            return new ClrExplainResult(parameterRowType, explanation);

            static RelDataType requireResultType(RelDataType? type) =>
                type ?? throw new java.lang.IllegalStateException("an EXPLAIN of a type needs one");
        }

        /// <summary>
        /// Creates the validator.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.createSqlValidator</c>, which is a private static there.
        /// </remarks>
        SqlValidator CreateSqlValidator(CalciteCatalogReader catalogReader, Func<SqlValidator.Config, SqlValidator.Config> configTransform)
        {
            var connectionConfig = Context.config();
            var opTab0 = (SqlOperatorTable)connectionConfig.fun((java.lang.Class)typeof(SqlOperatorTable), org.apache.calcite.sql.fun.SqlStdOperatorTable.instance());

            var list = new java.util.ArrayList();
            list.add(opTab0);
            list.add(catalogReader);

            var opTab = org.apache.calcite.sql.util.SqlOperatorTables.chain(list);

            var config = configTransform(
                SqlValidator.Config.DEFAULT
                    .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                    .withConformance(connectionConfig.conformance())
                    .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                    .withIdentifierExpansion(true));

            return new CalciteSqlValidator(opTab, catalogReader, typeFactory, config);
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.expandView</c>. A view's definition is parsed and converted against the
        /// schema path the view was declared with, which is not necessarily the connection's.
        ///
        /// <para>The validator is built with <c>withEmbeddedQuery(true)</c> and is not the cached one: a
        /// view's query is validated in its own right, against its own catalog reader.</para>
        /// </remarks>
        public RelRoot expandView(RelDataType rowType, string queryString, java.util.List schemaPath, java.util.List viewPath)
        {
            var parser = SqlParser.create(queryString, ParserConfig());

            SqlNode sqlNode;
            try
            {
                sqlNode = parser.parseQuery();
            }
            catch (SqlParseException e)
            {
                throw new java.lang.RuntimeException("parse failed", e);
            }

            var viewCatalogReader = CatalogReader.withSchemaPath(schemaPath);
            var viewValidator = CreateSqlValidator(viewCatalogReader, c => c.withEmbeddedQuery(true));
            var config = SqlToRelConverter.config().withTrimUnusedFields(true);
            var sqlToRelConverter = GetSqlToRelConverter(viewValidator, viewCatalogReader, config);

            return sqlToRelConverter.convertQuery(sqlNode, true, true);
        }

        /// <summary>
        /// Returns the parser configuration a view's definition is parsed with.
        /// </summary>
        /// <remarks>
        /// <c>CalcitePrepareImpl.createParser(String)</c>, which uses the default configuration rather than
        /// the connection's — a view's text was written against Calcite's own dialect.
        /// </remarks>
        static SqlParser.Config ParserConfig()
        {
            return SqlParser.config();
        }

    }

}
