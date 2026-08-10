using System;
using System.Linq;
using System.Reflection;

using org.apache.calcite.rel.metadata.janino;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Which of the five shapes a metadata call's cache key takes, and the objects that shape needs.
    /// </summary>
    /// <remarks>
    /// <c>CacheGeneratorUtil.CacheKeyStrategy</c>, which picks a shape from the method's parameters, writes
    /// the keys it needs as fields of the generated class, and writes the block that chooses between them
    /// into each call. Only the objects are built here; the block is emitted by
    /// <see cref="ClrMetadataHandlerEmitter"/>.
    ///
    /// <para>A key is compared by reference — <see cref="DescriptiveCacheKey"/> says so and overrides
    /// nothing — so each is built once, when the handler is built, and the generated class holds it.</para>
    /// </remarks>
    static class ClrMetadataCacheKey
    {

        /// <summary>
        /// The lowest and highest int a key is kept ready for.
        /// </summary>
        public const int Min = -256;
        public const int Max = 256;

        /// <summary>
        /// The five shapes.
        /// </summary>
        public enum Strategy
        {

            /// <summary>One key, for a method taking nothing but the rel and the query.</summary>
            NoArg,

            /// <summary>One key per value of a single boolean argument.</summary>
            Boolean,

            /// <summary>One key per constant of a single enum argument, and one for null.</summary>
            Enum,

            /// <summary>One key per int in a fixed range, and a list outside it.</summary>
            Int,

            /// <summary>A list of the method's key and its arguments.</summary>
            List,

        }

        /// <summary>
        /// A shape and what it holds.
        /// </summary>
        /// <param name="Kind"></param>
        /// <param name="Constants">
        /// The objects the block reads: one key for <see cref="Strategy.NoArg"/> and
        /// <see cref="Strategy.List"/>, the true and the false key for <see cref="Strategy.Boolean"/>, the
        /// null key and the table for <see cref="Strategy.Enum"/>, and the key and the table for
        /// <see cref="Strategy.Int"/>.
        /// </param>
        public readonly record struct Plan(Strategy Kind, object[] Constants);

        /// <summary>
        /// Returns the shape a call of <paramref name="method"/> is cached under.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
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
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        static DescriptiveCacheKey Describe(MethodInfo method, string arguments)
        {
            return new DescriptiveCacheKey($"{method.ReturnType.Name} {method.DeclaringType!.Name}.{method.Name}({arguments})");
        }

        /// <summary>
        /// Returns the parameter types of <paramref name="method"/>, as the description names them.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        static string Arguments(MethodInfo method)
        {
            return string.Join(", ", method.GetParameters().Select(p => p.ParameterType.Name));
        }

    }

}
