using System;
using System.Data.Common;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.sql.type;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Relational operator that converts a tree of <see cref="AdoConvention"/> nodes into a
    /// <see cref="ClrEnumerableConvention"/> result by executing the generated SQL against the underlying
    /// ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="AdoToEnumerableConverter"/>, and it generates the same SQL by the same
    /// route. What differs is the shape of the result: the rows are read by
    /// <see cref="AdoSequences.Read{TRow}"/> into an <see cref="System.Collections.Generic.IEnumerable{T}"/>
    /// and the row builder is a delegate, where the other builds a linq4j <c>Enumerable</c> and a
    /// <c>Function1</c>. A plan that ends here has no linq4j enumerator between the data reader and the
    /// operator above it.
    ///
    /// <para>Opening the connection and filling the command's parameters happen once, so the expressions for
    /// the data source and the enricher are Calcite's own, translated where they are built. Only the row
    /// builder and the sequence are on the per-row path, and those are this convention's.</para>
    /// </remarks>
    public class AdoToClrEnumerableConverter : ConverterImpl, ClrEnumerableRel
    {

        static readonly System.Reflection.MethodInfo ReadMethod = typeof(AdoSequences).GetMethod(nameof(AdoSequences.Read))
            ?? throw new InvalidOperationException($"'{nameof(AdoSequences.Read)}' is missing from {nameof(AdoSequences)}.");

        static readonly System.Reflection.MethodInfo GetDbReaderValueMethod = typeof(AdoReaderUtil).GetMethod(nameof(AdoReaderUtil.GetDbReaderValue), [typeof(DbDataReader), typeof(int), typeof(SqlTypeName)])
            ?? throw new InvalidOperationException($"'{nameof(AdoReaderUtil.GetDbReaderValue)}' is missing from {nameof(AdoReaderUtil)}.");

        static readonly System.Reflection.MethodInfo CreateEnricherMethod = typeof(AdoEnumerable).GetMethod(nameof(AdoEnumerable.CreateEnricher), [typeof(AdoDataSource), typeof(java.util.List), typeof(DataContext)])
            ?? throw new InvalidOperationException($"'{nameof(AdoEnumerable.CreateEnricher)}' is missing from {nameof(AdoEnumerable)}.");

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public AdoToClrEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new AdoToClrEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (getInput() is not AdoRel self)
                throw new AdoCalciteException("Unsupported input type.");

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var rowType = physType.RowType;

            if (self.getConvention() is not AdoConvention convention)
                throw new AdoCalciteException($"getConvention() is null for {self}.");

            var dataContextBuilder = new AdoClrCorrelationDataContextBuilder(implementor, implementor.Root);

            var writer = GenerateSql(convention, dataContextBuilder, self);
            var parameters = writer.Indexes;

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            // the schema SPI defines a convention's expression as linq4j, so this is the one thing here that
            // arrives as a linq4j tree, and it is translated where it is produced rather than composed into
            // anything first. Everything else this node builds is an expression tree from the start.
            var dataSource = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(AdoDataSource)));

            // a correlated sub-query leaves a parameter per correlation variable in the SQL, and the values
            // live on the context the builder closed over the outer row. Without the enricher the command is
            // handed to the provider unfilled.
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, CreateEnricherMethod, dataSource, Expression.Constant(parameters), dataContextBuilder.Build());

            return implementor.Result(physType,
                Expression.Call(null,
                    ReadMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    RowBuilder(physType, rowType),
                    enricher));
        }

        /// <summary>
        /// Builds the delegate that reads one row from the data reader.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The shape of a row is decided by how many fields it has, exactly as
        /// <see cref="AdoToEnumerableConverter"/> decides it, because <c>JavaRowFormat.optimize</c> has
        /// already told the physical type the same thing: no field is a null, one field is the value itself,
        /// and only beyond that is a row an array.
        /// </remarks>
        Expression RowBuilder(ClrPhysType physType, Type rowType)
        {
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var fieldCount = getRowType().getFieldCount();

            Expression body;
            if (fieldCount == 0)
                body = Expression.Constant(null, typeof(object));
            else if (fieldCount == 1)
                body = ReadField(reader, physType, 0);
            else
            {
                var values = new Expression[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                    values[i] = ReadField(reader, physType, i);

                body = Expression.NewArrayInit(typeof(object), values);
            }

            // the reader hands back a CLR value already, so what is left is the conversion a row of this
            // shape needs: unboxing where the row is one column of a value type, and nothing at all where it
            // is the Object[] every wider row is
            if (body.Type != rowType)
                body = Expression.Convert(body, rowType);

            return Expression.Lambda(typeof(Func<,>).MakeGenericType(typeof(DbDataReader), rowType), body, reader);
        }

        /// <summary>
        /// Returns the expression reading one field of the reader's current row.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="physType"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The declared SQL type decides how the value is read, not whatever the provider chose to surface it
        /// as, so the row holds what the plan was built against.
        /// </remarks>
        static Expression ReadField(ParameterExpression reader, ClrPhysType physType, int index)
        {
            var fieldType = ((RelDataTypeField)physType.RelRowType.getFieldList().get(index)).getType();

            return Expression.Call(null,
                GetDbReaderValueMethod,
                reader,
                Expression.Constant(index),
                Expression.Constant(fieldType.getSqlTypeName()));
        }

        /// <summary>
        /// Generates the SQL string to implement the sequence.
        /// </summary>
        /// <param name="convention"></param>
        /// <param name="dataContextBuilder"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        AdoSqlWriter GenerateSql(AdoConvention convention, IAdoCorrelationDataContextBuilder dataContextBuilder, AdoRel input)
        {
            var implementor = new AdoImplementor(convention.Dialect, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder);
            var result = implementor.visitRoot(input);

            var writer = new AdoSqlWriter(convention.Dialect, convention.Syntax);
            result.asStatement().unparse(writer, 0, 0);
            return writer;
        }

    }

}
