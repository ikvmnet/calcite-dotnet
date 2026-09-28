using System;
using System.Linq;
using System.Reflection;

using org.apache.calcite.rel.metadata.janino;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Chooses the cache-key strategy for a metadata method and builds the key objects it uses.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>CacheGeneratorUtil.CacheKeyStrategy</c>, which picks a strategy from the method's
    /// parameters and writes its keys as fields of the generated handler. The keys are built here; the code
    /// that selects among them is emitted by <see cref="ClrMetadataHandlerEmitter"/>.
    ///
    /// <para>A <see cref="DescriptiveCacheKey"/> is compared by reference, so each key is built once per
    /// handler and held by it.</para>
    /// </remarks>
    static class ClrMetadataCacheKey
    {

        /// <summary>
        /// The lowest int argument that has a precomputed key.
        /// </summary>
        public const int Min = -256;
        /// <summary>
        /// The highest int argument that has a precomputed key.
        /// </summary>
        public const int Max = 256;

        /// <summary>
        /// The cache-key strategies.
        /// </summary>
        public enum Strategy
        {

            /// <summary>One key, for a method taking nothing but the rel and the query.</summary>
            NoArg,

            /// <summary>One key per value of a single boolean argument.</summary>
            Boolean,

            /// <summary>One key per constant of a single enum argument, and one for null.</summary>
            Enum,

            /// <summary>One key per int from <see cref="Min"/> to <see cref="Max"/>, and a list outside that range.</summary>
            Int,

            /// <summary>A list of the method's key and its arguments.</summary>
            List,

        }

        /// <summary>
        /// A strategy and the key objects it uses.
        /// </summary>
        /// <param name="Kind">The strategy.</param>
        /// <param name="Constants">
        /// The key objects: one key for <see cref="Strategy.NoArg"/> and <see cref="Strategy.List"/>, the true
        /// and false keys for <see cref="Strategy.Boolean"/>, the null key and the per-constant table for
        /// <see cref="Strategy.Enum"/>, and the list key and the per-int table for <see cref="Strategy.Int"/>.
        /// </param>
        public readonly record struct Plan(Strategy Kind, object[] Constants);

        /// <summary>
        /// Returns the strategy a call of <paramref name="method"/> is cached under.
        /// </summary>
        /// <exception cref="ArgumentException"><paramref name="method"/> takes fewer than two parameters (the rel
        /// and the query).</exception>
        /// <param name="method">A handler method whose first two parameters are the rel and the metadata query.</param>
        /// <returns>The strategy, chosen by the type of the one argument after those two if there is one, and the keys it caches under.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="method"/> is <see langword="null"/>.</exception>
        public static Plan Of(MethodInfo method)
        {
            ArgumentNullException.ThrowIfNull(method);

            var parameters = method.GetParameters();
            if (parameters.Length < 2)
                throw new ArgumentException($"'{method}' takes fewer than the rel and the query.", nameof(method));

            if (parameters.Length == 2)
                return new Plan(Strategy.NoArg, [Describe(method, "")]);

            if (parameters.Length == 3)
            {
                var type = parameters[2].ParameterType;

                if (type == typeof(bool))
                    return new Plan(Strategy.Boolean, [Describe(method, "true"), Describe(method, "false")]);

                if (typeof(java.lang.Enum).IsAssignableFrom(type))
                {
                    var values = (Array)type.GetMethod("values", BindingFlags.Public | BindingFlags.Static, null, Type.EmptyTypes, null)!.Invoke(null, null)!;
                    return new Plan(Strategy.Enum,
                    [
                        Describe(method, "null"),
                        CacheUtil.generateEnum($"{method.ReturnType.Name} {method.Name}", (java.lang.Enum[])values)
                    ]);
                }

                if (type == typeof(int))
                    return new Plan(Strategy.Int,
                    [
                        Describe(method, Arguments(method)),
                        CacheUtil.generateRange($"{((java.lang.Class)method.ReturnType).getName()} {method.Name}", Min, Max)
                    ]);
            }

            return new Plan(Strategy.List, [Describe(method, Arguments(method))]);
        }

        /// <summary>
        /// Returns the key describing <paramref name="method"/> called with <paramref name="arguments"/>.
        /// </summary>
        /// <param name="method">The handler method.</param>
        /// <param name="arguments">The argument text to show between the parentheses.</param>
        /// <returns>A key whose description is the return type, declaring type, method name and <paramref name="arguments"/>.</returns>
        static DescriptiveCacheKey Describe(MethodInfo method, string arguments)
        {
            return new DescriptiveCacheKey($"{method.ReturnType.Name} {method.DeclaringType!.Name}.{method.Name}({arguments})");
        }

        /// <summary>
        /// Returns the parameter type names of <paramref name="method"/>, comma-separated, for a key's description.
        /// </summary>
        /// <param name="method">The handler method.</param>
        /// <returns>The CLR names of every parameter type, the rel and the query included.</returns>
        static string Arguments(MethodInfo method)
        {
            return string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name));
        }

    }

}
