using System;

using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// What a resolver may consult while answering a lookup.
    /// </summary>
    /// <remarks>
    /// The type factory is here rather than reached for globally because it is the thing that decides what a
    /// Calcite type is held in, and a session is entitled to its own — a schema that types a column with
    /// <c>createJavaType</c>, or a factory that overrides <c>getJavaClass</c>, changes the answer a mapping
    /// has to agree with. The registry is here so a resolver can compose: a mapping for a collection asks
    /// for its element's mapping rather than knowing every element type itself.
    /// </remarks>
    public sealed class ClrTypeContext
    {

        readonly JavaTypeFactory _typeFactory;
        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="registry"></param>
        internal ClrTypeContext(JavaTypeFactory typeFactory, ClrTypeRegistry registry)
        {
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        }

        /// <summary>
        /// Gets the type factory this lookup is being answered against.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Gets the registry the lookup was made on, for a resolver that composes.
        /// </summary>
        public ClrTypeRegistry Registry => _registry;

    }

}
