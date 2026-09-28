using org.apache.calcite.rex;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// A deep <see cref="RexVisitorImpl"/> that records whether an expression calls a user-defined function, which
    /// a source cannot evaluate. Mirrors <c>JdbcRules.CheckingUserDefinedFunctionVisitor</c>.
    /// </summary>
    public class CheckingUserDefinedFunctionVisitor : RexVisitorImpl
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CheckingUserDefinedFunctionVisitor() :
            base(true)
        {

        }

        /// <summary>
        /// Gets whether any expression visited so far calls a user-defined function.
        /// </summary>
        public bool ContainerUserDefinedFunction { get; private set; }

        /// <summary>
        /// Records the call if its operator is a user-defined <see cref="SqlFunction"/>, then visits its operands.
        /// </summary>
        /// <param name="call">The call.</param>
        /// <returns>The base visitor's result.</returns>
        public override object visitCall(RexCall call)
        {
            var op = call.getOperator();
            if (op is SqlFunction func && func.getFunctionType().isUserDefined())
                ContainerUserDefinedFunction |= true;

            return base.visitCall(call);
        }

    }

}
