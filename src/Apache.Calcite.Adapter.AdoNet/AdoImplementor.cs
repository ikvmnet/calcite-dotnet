using java.lang.reflect;
using java.util;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.rel2sql;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;

namespace Apache.Calcite.Adapter.AdoNet
{

    public class AdoImplementor : RelToSqlConverter
    {

        class Builder : IAdoCorrelationDataContextBuilder
        {

            int counter = 1;

            public int Add(CorrelationId id, int ordinal, Type type)
            {
                return counter++;
            }

        }


        readonly JavaTypeFactory _typeFactory;
        readonly IAdoCorrelationDataContextBuilder _dataContextBuilder;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="typeFactory"></param>
        /// <param name="dataContextBuilder"></param>
        AdoImplementor(SqlDialect dialect, JavaTypeFactory typeFactory, IAdoCorrelationDataContextBuilder dataContextBuilder) :
            base(dialect)
        {
            _typeFactory = typeFactory;
            _dataContextBuilder = dataContextBuilder;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="typeFactory"></param>
        public AdoImplementor(SqlDialect dialect, JavaTypeFactory typeFactory) :
            this(dialect, typeFactory, new Builder())
        {

        }

        public Result implement(RelNode node)
        {
            return dispatch(node);
        }

        protected Context getAliasContext(RexCorrelVariable variable)
        {
            var context = (Context)correlTableMap.get(variable.id);
            if (context != null)
                return context;

            var fieldList = variable.getType().getFieldList();
            return new DefaultContext(this, variable, fieldList);
        }

        class DefaultContext : Context
        {

            readonly AdoImplementor _implementor;
            readonly RexCorrelVariable _variable;
            readonly List _fieldList;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="implementor"></param>
            /// <param name="variable"></param>
            /// <param name="fieldList"></param>
            public DefaultContext(AdoImplementor implementor, RexCorrelVariable variable, List fieldList) :
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
                return new SqlDynamicParam(_implementor._dataContextBuilder.Add(_variable.id, ordinal, _implementor._typeFactory.getJavaClass(field.getType())), SqlParserPos.ZERO);
            }

        }

    }

}
