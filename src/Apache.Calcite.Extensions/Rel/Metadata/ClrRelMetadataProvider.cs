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
        /// Answers with a handler that refuses every rel, so that a query builds only the handlers it reaches.
        /// </summary>
        /// <param name="handlerClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// What Janino's provider does, and for the same reason: <see cref="RelMetadataQuery"/> asks for all
        /// twenty-seven when it is constructed, which is once per query, and a statement touches a third of
        /// them. The refusal is caught there and answered by <see cref="revise"/>.
        /// </remarks>
        public MetadataHandler handler(java.lang.Class handlerClass)
        {
            ArgumentNullException.ThrowIfNull(handlerClass);

            return (MetadataHandler)java.lang.reflect.Proxy.newProxyInstance(
                ((java.lang.Class)typeof(RelMetadataQuery)).getClassLoader(),
                [handlerClass],
                new Refusing());
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
        /// The handler <see cref="handler"/> answers with, which refuses every rel it is asked about.
        /// </summary>
        sealed class Refusing : java.lang.reflect.InvocationHandler
        {

            /// <inheritdoc />
            public object invoke(object proxy, java.lang.reflect.Method method, object[] args)
            {
                var r = (RelNode)args[0];
                throw new MetadataHandlerProvider.NoHandler((java.lang.Class)r.GetType());
            }

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
