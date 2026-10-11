using System;
using System.Collections.Generic;

using org.apache.calcite.config;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The allowlist every class a model names by reflection passes before it is loaded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The allowlist is <c>calcite.model.classes.allowed</c>, read once from <see cref="CalciteSystemProperty"/>,
    /// the property Calcite's <c>ClassNameFilter</c> reads. A pattern ending in <c>.</c> matches a package and
    /// its sub-packages; any other matches one class. A <c>#FIELD</c> suffix is ignored. A class no pattern
    /// matches is refused.
    /// </para>
    /// <para>
    /// Calcite's filter also has a denylist, which refuses some classes whatever the allowlist says. It is not
    /// applied here: a class loads where the allowlist names it. User-defined functions go through
    /// <c>ModelHandler.addFunctions</c>, which applies Calcite's own filter.
    /// </para>
    /// </remarks>
    internal sealed class ModelClassFilter
    {

        static readonly Lazy<ModelClassFilter> standard = new(() => new ModelClassFilter(
            (string)CalciteSystemProperty.MODEL_CLASSES_ALLOWED.value()));

        /// <summary>
        /// Gets the filter built from <c>calcite.model.classes.allowed</c>.
        /// </summary>
        public static ModelClassFilter Standard => standard.Value;

        readonly IReadOnlyList<string> _allowlist;

        ModelClassFilter(string allowlist)
        {
            _allowlist = Parse(allowlist);
        }

        /// <summary>
        /// Throws where a class a model names is not allowed. A <see langword="null"/> reference is allowed.
        /// </summary>
        /// <param name="classRef">The class name, optionally followed by <c>#</c> and a field name.</param>
        /// <exception cref="java.lang.SecurityException">No pattern of the allowlist matches the class.</exception>
        public void Check(string? classRef)
        {
            if (classRef is null)
                return;

            var hash = classRef.IndexOf('#');
            var className = hash >= 0 ? classRef.Substring(0, hash) : classRef;

            foreach (var pattern in _allowlist)
                if (Matches(pattern, className))
                    return;

            throw new java.lang.SecurityException($"Class '{className}' rejected by the allowlist.");
        }

        static bool Matches(string pattern, string className)
        {
            return pattern.EndsWith('.') ? className.StartsWith(pattern, StringComparison.Ordinal) : className == pattern;
        }

        static IReadOnlyList<string> Parse(string list)
        {
            var patterns = new List<string>();
            foreach (var s in list.Split(','))
            {
                var trimmed = s.Trim();
                if (trimmed.Length > 0)
                    patterns.Add(trimmed);
            }

            return patterns;
        }

    }

}
