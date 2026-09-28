using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Supplies the metadata handlers a <see cref="RelMetadataQuery"/> uses, emitted as CLR types rather than
    /// generated as Java source and compiled with Janino.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>JaninoRelMetadataProvider</c>, with the same behaviour: handlers
    /// come from the given <see cref="RelMetadataProvider"/>, dispatch on the rel's class follows the same
    /// rule, and results are cached in the query's table under the same keys, with the same cycle
    /// detection. Each handler is built once per provider and handler interface, without running a Java
    /// compiler.
    ///
    /// <para>Install it on a cluster with <see cref="RelOptCluster.setMetadataQuerySupplier"/> and
    /// <see cref="QuerySupplier"/>. <c>RelMetadataQueryBase.THREAD_PROVIDERS</c> is typed to Janino's
    /// provider and cannot hold this one.</para>
    /// </remarks>
    public sealed class ClrRelMetadataProvider : MetadataHandlerProvider
    {

        /// <summary>
        /// A provider over <c>DefaultRelMetadataProvider.INSTANCE</c>, Calcite's own handlers.
        /// </summary>
        public static readonly ClrRelMetadataProvider Default = Of(DefaultRelMetadataProvider.INSTANCE);

        static readonly ConcurrentDictionary<(RelMetadataProvider, Type), MetadataHandler> handlers = new();

        /// <summary>
        /// Returns a provider over <paramref name="provider"/>'s handlers.
        /// </summary>
        /// <param name="provider">The source of the handlers.</param>
        /// <returns>The provider. Providers over equal sources are equal and share built handlers.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="provider"/> is
        /// <see langword="null"/>.</exception>
        public static ClrRelMetadataProvider Of(RelMetadataProvider provider)
        {
            ArgumentNullException.ThrowIfNull(provider);

            return new ClrRelMetadataProvider(provider);
        }

        /// <summary>
        /// Returns a supplier of queries over <paramref name="provider"/>, for
        /// <see cref="RelOptCluster.setMetadataQuerySupplier"/>.
        /// </summary>
        /// <param name="provider">The source of the handlers.</param>
        /// <returns>A supplier that returns a new <see cref="RelMetadataQuery"/> on each call, as a cluster
        /// requires.</returns>
        public static java.util.function.Supplier QuerySupplier(RelMetadataProvider provider)
        {
            return new Supplier(Of(provider));
        }

        readonly RelMetadataProvider provider;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="provider">The source of the handlers.</param>
        ClrRelMetadataProvider(RelMetadataProvider provider)
        {
            this.provider = provider;
        }

        /// <summary>
        /// Returns the handler for <paramref name="handlerClass"/>.
        /// </summary>
        /// <param name="handlerClass">The handler interface.</param>
        /// <returns>The built handler.</returns>
        /// <remarks>
        /// Returns the built handler directly, where Janino's provider returns a proxy that throws
        /// <c>NoHandler</c> so that a handler is compiled only when first used. A <see cref="RelMetadataQuery"/>
        /// asks for every handler each time one is constructed, which the planner does often, and a built
        /// handler here is cached, so there is nothing to gain from deferring.
        /// </remarks>
        public MetadataHandler handler(java.lang.Class handlerClass)
        {
            return revise(handlerClass);
        }

        /// <summary>
        /// Returns the handler for <paramref name="handlerClass"/>, building it the first time it is asked
        /// for.
        /// </summary>
        /// <param name="handlerClass">The handler interface.</param>
        /// <returns>The built handler, shared by every provider over an equal source.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="handlerClass"/> is
        /// <see langword="null"/>.</exception>
        public MetadataHandler revise(java.lang.Class handlerClass)
        {
            ArgumentNullException.ThrowIfNull(handlerClass);

            var type = (Type)ikvm.runtime.Util.getInstanceTypeFromClass(handlerClass);
            return handlers.GetOrAdd((provider, type), k => Build(k.Item1, k.Item2, handlerClass));
        }

        /// <inheritdoc />
        public override bool Equals(object? obj)
        {
            return ReferenceEquals(this, obj) || (obj is ClrRelMetadataProvider other && provider.Equals(other.provider));
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return 109 + provider.GetHashCode();
        }

        /// <summary>
        /// Builds the handler for <paramref name="handlerClass"/> over <paramref name="provider"/>.
        /// </summary>
        /// <param name="provider">The provider whose handlers the new handler delegates to.</param>
        /// <param name="type">The CLR type of the handler interface.</param>
        /// <param name="handlerClass">The Java class of the handler interface, as <c>RelMetadataProvider.handlers</c> takes it.</param>
        /// <returns>An emitted handler that dispatches each method to the first of the provider's distinct handlers declaring it for the rel's class.</returns>
        static MetadataHandler Build(RelMetadataProvider provider, Type type, java.lang.Class handlerClass)
        {
            // handlers().stream().distinct(), as in Calcite: order matters, since the first handler to declare
            // a rel class answers for it
            var list = provider.handlers(handlerClass);
            var underlying = new List<MetadataHandler>(list.size());
            for (int i = 0; i < list.size(); i++)
            {
                var handler = (MetadataHandler)list.get(i);
                if (underlying.Contains(handler) is false)
                    underlying.Add(handler);
            }

            return ClrMetadataHandlerEmitter.Emit(type, Methods(type), underlying);
        }

        /// <summary>
        /// Returns the methods of a handler interface, in the order Calcite indexes them.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>MetadataHandler.handlerMethods</c>: the interface's abstract instance methods other than
        /// <c>getDef</c>, ordered by name, read as CLR methods.
        /// </remarks>
        /// <param name="type">The CLR type of the handler interface.</param>
        /// <returns>The methods, ordered by name.</returns>
        static MethodInfo[] Methods(Type type)
        {
            return type.GetMethods()
                .Where(m => m.Name != "getDef" && m.IsAbstract && !m.IsStatic)
                .OrderBy(m => m.Name, StringComparer.Ordinal)
                .ToArray();
        }

        /// <summary>
        /// Supplies a new query over a provider on each call.
        /// </summary>
        /// <param name="provider">The provider each new query reads metadata from.</param>
        sealed class Supplier(ClrRelMetadataProvider provider) : java.util.function.Supplier
        {

            /// <inheritdoc />
            public object get() => new RelMetadataQuery(provider);

        }

    }

}
