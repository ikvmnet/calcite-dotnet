using org.apache.calcite.rex;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    /// <summary>
    /// Visitor that checks whether part of a projection is a user-defined function (UDF).
    /// </summary>
    class CheckingUserDefinedFunctionVisitor : RexVisitorImpl
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CheckingUserDefinedFunctionVisitor() :
            base(true)
        {

        }

        /// <summary>
        /// Gets whether or not the visitor encountered a user defined function.
        /// </summary>
        public bool ContainerUserDefinedFunction { get; private set; }

        /// <inheritdoc />
        public override object visitCall(RexCall call)
        {
            var op = call.getOperator();
            if (op is SqlFunction func && func.getFunctionType().isUserDefined())
                ContainerUserDefinedFunction |= true;

            return base.visitCall(call);
        }

    }

}
