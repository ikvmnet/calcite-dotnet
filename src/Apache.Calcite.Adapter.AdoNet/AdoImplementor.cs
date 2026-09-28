using System;

using Apache.Calcite.Adapter.AdoNet.Rel;

using java.util;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Translates a tree of <see cref="AdoRel"/> nodes into a SQL statement. The counterpart of Calcite's
    /// <c>JdbcImplementor</c>.
    /// </summary>
    /// <remarks>
    /// A correlation variable that the tree reads from an enclosing query, rather than from a correlation set up
    /// inside the tree, is written as a dynamic parameter. Each is registered with the
    /// <see cref="IAdoCorrelationDataContextBuilder"/>, which assigns its index, and its SQL type is recorded for
    /// <see cref="GetDynamicParamType"/>.
    /// </remarks>
    public class AdoImplementor : RelToSqlConverter
    {

        /// <summary>
        /// Numbers correlation variables from 1 without recording them, for an implementor constructed without a
        /// builder.
        /// </summary>
        class _Builder : IAdoCorrelationDataContextBuilder
        {

            int counter = 1;

            public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type)
            {
                return counter++;
            }

        }

        /// <summary>
        /// The alias context of a correlation variable from outside the tree, which writes each field it is asked
        /// for as a dynamic parameter.
        /// </summary>
        class _Context : Context
        {

            readonly AdoImplementor _implementor;
            readonly RexCorrelVariable _variable;
            readonly List _fieldList;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="implementor">The implementor.</param>
            /// <param name="variable">The correlation variable.</param>
            /// <param name="fieldList">The variable's fields.</param>
            public _Context(AdoImplementor implementor, RexCorrelVariable variable, List fieldList) :
                base(implementor.dialect, fieldList.size())
            {
                _implementor = implementor;
                _variable = variable;
                _fieldList = fieldList;
            }

            public override SqlImplementor implementor()
            {
                return _implementor;
            }

            public override SqlNode field(int ordinal)
            {
                var field = (RelDataTypeField)_fieldList.get(ordinal);
                var index = _implementor._dataContextBuilder.Add(_variable.id, ordinal, _implementor._typeFactory.getJavaClass(field.getType()));

                // the value arrives in Calcite's representation (a DATE is a day count in an Integer), and only
                // the SQL type tells the binder how to decode it
                _implementor._dynamicParamTypes[index] = field.getType().getSqlTypeName();

                return new SqlDynamicParam(index, SqlParserPos.ZERO);
            }

        }

        readonly JavaTypeFactory _typeFactory;
        readonly IAdoCorrelationDataContextBuilder _dataContextBuilder;
        readonly System.Collections.Generic.Dictionary<int, SqlTypeName> _dynamicParamTypes = [];

        /// <summary>
        /// Returns the SQL type of the correlation variable written as a dynamic parameter, or
        /// <see langword="null"/> for an index that is not one.
        /// </summary>
        /// <param name="index">The parameter's variable index.</param>
        /// <returns>The SQL type, or <see langword="null"/>.</returns>
        public SqlTypeName? GetDynamicParamType(int index)
        {
            return _dynamicParamTypes.TryGetValue(index, out var type) ? type : null;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect">The dialect SQL is written in.</param>
        /// <param name="typeFactory">The type factory, which gives each correlation variable field its Java class.</param>
        /// <param name="dataContextBuilder">Registers each correlation variable the statement reads and assigns its
        /// index.</param>
        /// <exception cref="ArgumentNullException"><paramref name="typeFactory"/> or
        /// <paramref name="dataContextBuilder"/> is <see langword="null"/>.</exception>
        public AdoImplementor(SqlDialect dialect, JavaTypeFactory typeFactory, IAdoCorrelationDataContextBuilder dataContextBuilder) :
            base(dialect)
        {
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            _dataContextBuilder = dataContextBuilder ?? throw new ArgumentNullException(nameof(dataContextBuilder));
        }

        /// <summary>
        /// Initializes a new instance that numbers correlation variables from 1 and registers them nowhere, so a
        /// statement it writes cannot be given their values.
        /// </summary>
        /// <param name="dialect">The dialect SQL is written in.</param>
        /// <param name="typeFactory">The type factory.</param>
        public AdoImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) :
            this(dialect, typeFactory, new _Builder())
        {

        }

        /// <summary>
        /// Sends an <see cref="AdoRel"/> to its own <see cref="AdoRel.implement"/>, and any other node to Calcite's
        /// translation.
        /// </summary>
        /// <param name="rel">The node.</param>
        /// <returns>The node's SQL.</returns>
        protected override Result dispatch(RelNode rel)
        {
            return rel is AdoRel ado ? ado.implement(this) : base.dispatch(rel);
        }

        /// <summary>
        /// Translates an <see cref="AdoRel"/> with Calcite's own translation for its node type. The default
        /// <see cref="AdoRel.implement"/> calls this.
        /// </summary>
        /// <param name="rel">The node.</param>
        /// <returns>The node's SQL.</returns>
        public Result implement(AdoRel rel)
        {
            return base.dispatch(rel);
        }

        /// <summary>
        /// Returns the alias context of a correlation variable: the one registered where the tree sets up the
        /// correlation, or otherwise one that writes the variable's fields as dynamic parameters.
        /// </summary>
        /// <param name="variable">The correlation variable.</param>
        /// <returns>The context.</returns>
        protected override Context getAliasContext(RexCorrelVariable variable)
        {
            var context = (Context)correlTableMap.get(variable.id);
            if (context != null)
                return context;

            var fieldList = variable.getType().getFieldList();
            return new _Context(this, variable, fieldList);
        }

    }

}
