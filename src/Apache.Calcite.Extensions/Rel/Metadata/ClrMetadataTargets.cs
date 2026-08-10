using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Which underlying handler answers a metadata method for a rel of a given class, and in what order the
    /// classes are tried.
    /// </summary>
    /// <remarks>
    /// <c>DispatchGenerator</c>. Calcite asks each handler which rel classes it declares the method for,
    /// orders the classes so that no class comes before one it is a supertype of, and writes an
    /// <c>instanceof</c> chain in that order; where two candidates are unrelated it is their Java names that
    /// decide, and where two handlers declare the same class the first handler wins.
    /// </remarks>
    static class ClrMetadataTargets
    {

        /// <summary>
        /// One branch of the chain.
        /// </summary>
        /// <param name="RelClass">The class the rel is tested against.</param>
        /// <param name="Provider">Which of the handlers answers.</param>
        /// <param name="Method">The method of that handler to call.</param>
        public readonly record struct Target(Type RelClass, int Provider, MethodInfo Method);

        /// <summary>
        /// Returns the chain for <paramref name="superMethod"/> over <paramref name="handlers"/>.
        /// </summary>
        /// <param name="superMethod"></param>
        /// <param name="handlers"></param>
        /// <returns></returns>
        public static Target[] Of(MethodInfo superMethod, IReadOnlyList<MetadataHandler> handlers)
        {
            ArgumentNullException.ThrowIfNull(superMethod);
            ArgumentNullException.ThrowIfNull(handlers);

            var declared = handlers.Select(h => RelClasses(superMethod, h)).ToArray();
            var classes = TopologicalSort(declared.SelectMany(d => d).Distinct());

            return classes.Select(c =>
            {
                var provider = Array.FindIndex(declared, d => d.Contains(c));
                var method = handlers[provider].GetType().GetMethod(superMethod.Name, Parameters(superMethod, c))
                    ?? throw new InvalidOperationException($"'{handlers[provider].GetType()}' does not declare {superMethod.Name} for '{c}'.");

                return new Target(c, provider, method);
            }).ToArray();
        }

        /// <summary>
        /// Returns the parameters of the underlying method: the method's own, with the rel narrowed.
        /// </summary>
        /// <param name="superMethod"></param>
        /// <param name="relClass"></param>
        /// <returns></returns>
        static Type[] Parameters(MethodInfo superMethod, Type relClass)
        {
            var parameters = superMethod.GetParameters();
            var types = new Type[parameters.Length];
            types[0] = relClass;
            for (int i = 1; i < parameters.Length; i++)
                types[i] = parameters[i].ParameterType;

            return types;
        }

        /// <summary>
        /// Returns the rel classes <paramref name="handler"/> declares <paramref name="superMethod"/> for.
        /// </summary>
        /// <param name="superMethod"></param>
        /// <param name="handler"></param>
        /// <returns></returns>
        static HashSet<Type> RelClasses(MethodInfo superMethod, MetadataHandler handler)
        {
            var set = new HashSet<Type>();

            foreach (var candidate in handler.GetType().GetMethods())
                if (ToRelClass(superMethod, candidate) is Type relClass)
                    set.Add(relClass);

            return set;
        }

        /// <summary>
        /// Returns the rel class <paramref name="candidate"/> answers <paramref name="superMethod"/> for, or
        /// null where it does not answer it at all.
        /// </summary>
        /// <param name="superMethod"></param>
        /// <param name="candidate"></param>
        /// <returns></returns>
        static Type? ToRelClass(MethodInfo superMethod, MethodInfo candidate)
        {
            if (candidate.Name != superMethod.Name)
                return null;

            var cpt = candidate.GetParameters();
            var smpt = superMethod.GetParameters();
            if (cpt.Length != smpt.Length)
                return null;
            if (!typeof(RelNode).IsAssignableFrom(cpt[0].ParameterType))
                return null;
            if (cpt[1].ParameterType != typeof(RelMetadataQuery))
                return null;

            for (int i = 2; i < smpt.Length; i++)
                if (cpt[i].ParameterType != smpt[i].ParameterType)
                    return null;

            return cpt[0].ParameterType;
        }

        /// <summary>
        /// Orders <paramref name="classes"/> so that no class comes before one it is a supertype of.
        /// </summary>
        /// <param name="classes"></param>
        /// <returns></returns>
        /// <remarks>
        /// Ordering by the Java name, which is what decides between two candidates neither of which is the
        /// other's supertype — and is not the CLR name for a class of this project's own.
        /// </remarks>
        static List<Type> TopologicalSort(IEnumerable<Type> classes)
        {
            var sorted = new List<Type>();
            var remaining = new Queue<Type>(classes.OrderBy(t => ((java.lang.Class)t).getName(), StringComparer.Ordinal));

            while (remaining.Count > 0)
            {
                var next = remaining.Dequeue();
                if (remaining.Any(other => next.IsAssignableFrom(other)))
                    remaining.Enqueue(next);
                else
                    sorted.Add(next);
            }

            return sorted;
        }

    }

}
