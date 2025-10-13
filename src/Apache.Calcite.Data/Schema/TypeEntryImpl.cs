using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Impl
{

    /// <summary>
    /// Membership of a type in a schema.
    /// </summary>
    public class TypeEntryImpl : TypeEntry
    {

        readonly RelProtoDataType _type;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="type"></param>
        public TypeEntryImpl(CalciteSchema schema, string name, RelProtoDataType type) :
            base(schema, name)
        {
            this._type = type ?? throw new ArgumentNullException(nameof(type));
        }

        /// <inheritdoc />
        public override RelProtoDataType Type => _type;

    }

}
