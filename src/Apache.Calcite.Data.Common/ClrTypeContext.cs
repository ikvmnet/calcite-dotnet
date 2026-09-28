using System;

using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// What a resolver can consult while answering a lookup.
    /// </summary>
    /// <remarks>
    /// The type factory decides which runtime class Calcite holds a value of each type in, and each session
    /// has its own, so a resolver takes it from here rather than assuming one. The registry lets a resolver
    /// compose, for example by looking up a collection's element mapping.
    /// </remarks>
    public sealed class ClrTypeContext
    {

        readonly JavaTypeFactory _typeFactory;
        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory">The type factory lookups are answered against.</param>
        /// <param name="registry">The registry that owns this context.</param>
        internal ClrTypeContext(JavaTypeFactory typeFactory, ClrTypeRegistry registry)
        {
            _typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            _registry = registry ?? throw new ArgumentNullException(nameof(registry));
        }

        /// <summary>
        /// Gets the type factory the lookup is answered against.
        /// </summary>
        public JavaTypeFactory TypeFactory => _typeFactory;

        /// <summary>
        /// Gets the registry the lookup was made on, through which a resolver can look up other mappings.
        /// </summary>
        public ClrTypeRegistry Registry => _registry;

    }

}
