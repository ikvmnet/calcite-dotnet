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
    /// Provides the metadata handlers a <see cref="RelMetadataQuery"/> asks for, built as CLR types and
    /// <c>System.Linq.Expressions</c> rather than generated as Java source and compiled by Janino.
    /// </summary>
    /// <remarks>
    /// <c>JaninoRelMetadataProvider</c>, and it answers the same questions the same way: the handlers come
    /// from the same <see cref="RelMetadataProvider"/>, the class a rel dispatches to is chosen by the same
    /// rule, and the results are cached in the query's own table under the same keys, cycles included.
    ///
    /// <para>Two things move. A handler is built once per provider and handler interface, as Janino's is, but
    /// building one no longer runs a Java compiler. And a handler written in .NET works: Calcite's generated
    /// source names the handler class and each rel class in Java, and IKVM's name for a CLR class begins
    /// <c>cli.</c>, which Janino refuses — "Cannot determine simple type name cli", measured. Nothing here
    /// writes a name.</para>
    ///
    /// <para>Reached through <see cref="RelOptCluster.setMetadataQuerySupplier"/>:
    /// <c>RelMetadataQueryBase.THREAD_PROVIDERS</c> is typed to Janino's provider and cannot hold this one,
    /// and <c>RelMetadataQuery.instance()</c> is the only thing that reads it.</para>
    /// </remarks>
    public sealed class ClrRelMetadataProvider : MetadataHandlerProvider
    {

        /// <summary>
        /// The provider over Calcite's own handlers.
        /// </summary>
        public static readonly ClrRelMetadataProvider Default = Of(DefaultRelMetadataProvider.INSTANCE);

        static readonly ConcurrentDictionary<(RelMetadataProvider, Type), MetadataHandler> handlers = new();

        /// <summary>
        /// Returns a provider over <paramref name="provider"/>'s handlers.
        /// </summary>
        /// <param name="provider"></param>
        /// <returns></returns>
        public static ClrRelMetadataProvider Of(RelMetadataProvider provider)
        {
            ArgumentNullException.ThrowIfNull(provider);

            return new ClrRelMetadataProvider(provider);
        }

        /// <summary>
        /// Returns a supplier of queries over <paramref name="provider"/>, for
        /// <see cref="RelOptCluster.setMetadataQuerySupplier"/>, which requires a fresh query each time.
        /// </summary>
        /// <param name="provider"></param>
        /// <returns></returns>
        public static java.util.function.Supplier QuerySupplier(RelMetadataProvider provider)
        {
            return new Supplier(Of(provider));
        }

        readonly RelMetadataProvider provider;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="provider"></param>
        ClrRelMetadataProvider(RelMetadataProvider provider)
        {
            this.provider = provider;
        }

        /// <summary>
        /// Returns the handler of <paramref name="handlerClass"/>.
        /// </summary>
        /// <param name="handlerClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// The handler itself, where Janino's provider answers a <c>java.lang.reflect.Proxy</c> that throws
        /// <c>NoHandler</c> so that <see cref="revise"/> builds one only for the handlers a statement
        /// reaches. That deferral buys Janino a Java compile it may not have to run; there is nothing here to
        /// defer, because a handler is emitted once per provider and interface and then answered from a
        /// dictionary.
        ///
        /// <para>The proxy was also a cost and a hazard. <see cref="RelMetadataQuery"/> asks for all
        /// twenty-seven every time one is constructed, and the planner constructs one whenever a rule
        /// transforms — so the proxy route meant twenty-seven dynamic proxy classes and reflection accessors
        /// per transformation, built through IKVM's own <c>Reflection.Emit</c> path, which is where
        /// <c>InvalidProgramException</c> was coming from.</para>
        /// </remarks>
        public MetadataHandler handler(java.lang.Class handlerClass)
        {
            return revise(handlerClass);
        }

        /// <summary>
        /// Returns the handler of <paramref name="handlerClass"/>, building it the first time a provider is
        /// asked for it.
        /// </summary>
        /// <param name="handlerClass"></param>
        /// <returns></returns>
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
        /// Builds the handler of <paramref name="handlerClass"/> over <paramref name="provider"/>.
        /// </summary>
        /// <param name="provider"></param>
        /// <param name="type"></param>
        /// <param name="handlerClass"></param>
        /// <returns></returns>
        static MetadataHandler Build(RelMetadataProvider provider, Type type, java.lang.Class handlerClass)
        {
            // handlers().stream().distinct(): the first of each, in the order the provider gave them, which
            // is the order the chain resolves a rel class in
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
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>MetadataHandler.handlerMethods</c>: what the interface itself declares, less <c>getDef</c> and
        /// anything static or synthetic, by name. Reading it from the CLR rather than calling it keeps the
        /// members in the reflection the dispatch and the emitter are written in.
        /// </remarks>
        static MethodInfo[] Methods(Type type)
        {
            return type.GetMethods()
                .Where(m => m.Name != "getDef" && m.IsAbstract && !m.IsStatic)
                .OrderBy(m => m.Name, StringComparer.Ordinal)
                .ToArray();
        }

        /// <summary>
        /// Supplies a fresh query over this provider, which is what a cluster requires.
        /// </summary>
        /// <param name="provider"></param>
        sealed class Supplier(ClrRelMetadataProvider provider) : java.util.function.Supplier
        {

            /// <inheritdoc />
            public object get() => new RelMetadataQuery(provider);

        }

    }

}
