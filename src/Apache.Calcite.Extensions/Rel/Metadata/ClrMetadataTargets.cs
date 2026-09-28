using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Computes which underlying handler answers a metadata method for each rel class, and the order in which
    /// the classes are tested.
    /// </summary>
    /// <remarks>
    /// Mirrors Calcite's <c>DispatchGenerator</c>: collect the rel classes each handler declares the method
    /// for, order them so that no class precedes one of its subclasses (unrelated classes by Java name), and
    /// test them in that order. Where two handlers declare the same class, the first wins.
    /// </remarks>
    static class ClrMetadataTargets
    {

        /// <summary>
        /// One branch of the dispatch.
        /// </summary>
        /// <param name="RelClass">The class the rel is tested against.</param>
        /// <param name="Provider">The index of the handler that answers.</param>
        /// <param name="Method">The method of that handler to call.</param>
        public readonly record struct Target(Type RelClass, int Provider, MethodInfo Method);

        /// <summary>
        /// Returns the dispatch branches for <paramref name="superMethod"/> over <paramref name="handlers"/>,
        /// in the order they are tested.
        /// </summary>
        /// <param name="superMethod">The method of the handler interface being dispatched.</param>
        /// <param name="handlers">The handlers, in order; the first to declare a rel class answers for it.</param>
        /// <returns>One target per rel class any handler declares, ordered so that a subclass is tested before its superclasses.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="superMethod"/> or <paramref name="handlers"/> is <see langword="null"/>.</exception>
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
        /// Returns the parameter types of a handler's method: the interface method's, with the rel narrowed
        /// to <paramref name="relClass"/>.
        /// </summary>
        /// <param name="superMethod">The method of the handler interface.</param>
        /// <param name="relClass">The rel class the handler's method takes.</param>
        /// <returns>The parameter types, <paramref name="relClass"/> first.</returns>
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
        /// <param name="superMethod">The method of the handler interface.</param>
        /// <param name="handler">The handler whose public methods are searched.</param>
        /// <returns>The distinct rel classes, in no particular order.</returns>
        static HashSet<Type> RelClasses(MethodInfo superMethod, MetadataHandler handler)
        {
            var set = new HashSet<Type>();

            foreach (var candidate in handler.GetType().GetMethods())
                if (ToRelClass(superMethod, candidate) is Type relClass)
                    set.Add(relClass);

            return set;
        }

        /// <summary>
        /// Returns the rel class <paramref name="candidate"/> implements <paramref name="superMethod"/> for, or
        /// <see langword="null"/> if it is not an implementation of it.
        /// </summary>
        /// <param name="superMethod">The method of the handler interface.</param>
        /// <param name="candidate">A public method of a handler.</param>
        /// <returns>The type of the candidate's first parameter, or <see langword="null"/>.</returns>
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
        /// Orders <paramref name="classes"/> so that no class comes before one of its subclasses.
        /// </summary>
        /// <remarks>
        /// Unrelated classes are ordered by Java name, as in Calcite; for a .NET class that differs from the
        /// CLR name.
        /// </remarks>
        /// <param name="classes">The rel classes to order.</param>
        /// <returns>The classes, each subclass before its superclasses.</returns>
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
