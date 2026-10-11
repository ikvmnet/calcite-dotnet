using System;
using System.Collections.Generic;

using org.apache.calcite.config;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A port of Calcite's <c>ClassNameFilter.standard()</c>: the check every class a model names by reflection
    /// passes before it is loaded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <c>ClassNameFilter.standard()</c> and <c>ClassNameFilter.check</c> are package-private, and the model
    /// is applied here rather than by <c>ModelHandler</c>, so the filter is ported. It is Calcite's built-in
    /// denylist extended by <c>calcite.model.classes.denied</c>, and the allowlist
    /// <c>calcite.model.classes.allowed</c>, both read once from <see cref="CalciteSystemProperty"/>. A
    /// pattern ending in <c>.</c> matches a package and its sub-packages; any other matches one class. A
    /// <c>#FIELD</c> suffix is ignored. A class matching the denylist is refused; otherwise one matching the
    /// allowlist is accepted, and anything else refused.
    /// </para>
    /// <para>
    /// User-defined functions go through <c>ModelHandler.addFunctions</c>, which applies Calcite's own copy
    /// of this filter.
    /// </para>
    /// </remarks>
    internal sealed class ModelClassFilter
    {

        /// <summary>
        /// Calcite's <c>ClassNameFilter.DEFAULT_DENYLIST</c>: class-name patterns known to enable remote code
        /// execution when a model names them.
        /// </summary>
        const string DefaultDenylist = ""
            + "javax.naming.,"
            + "com.sun.jndi.,"
            + "java.lang.Runtime,"
            + "java.lang.ProcessBuilder,"
            + "java.lang.ProcessImpl,"
            + "java.lang.System,"
            + "java.lang.Class,"
            + "java.lang.reflect.,"
            + "java.lang.invoke.,"
            + "javax.script.,"
            + "bsh.,"
            + "groovy.,"
            + "org.codehaus.groovy.,"
            + "org.python.util.PythonInterpreter,"
            + "org.springframework.expression.,"
            + "org.apache.commons.collections.functors.,"
            + "org.apache.commons.collections4.functors.,"
            + "org.apache.commons.beanutils.,"
            + "com.sun.org.apache.xalan.internal.xsltc.trax.TemplatesImpl,"
            + "sun.misc.Unsafe,"
            + "jdk.internal.";

        static readonly Lazy<ModelClassFilter> standard = new(() => new ModelClassFilter(
            Append(DefaultDenylist, (string)CalciteSystemProperty.MODEL_CLASSES_DENIED.value()),
            (string)CalciteSystemProperty.MODEL_CLASSES_ALLOWED.value()));

        /// <summary>
        /// Gets the filter Calcite's <c>ModelHandler</c> uses by default.
        /// </summary>
        public static ModelClassFilter Standard => standard.Value;

        readonly IReadOnlyList<string> _denylist;
        readonly IReadOnlyList<string> _allowlist;

        ModelClassFilter(string denylist, string allowlist)
        {
            _denylist = Parse(denylist);
            _allowlist = Parse(allowlist);
        }

        /// <summary>
        /// Throws where a class a model names is not allowed. A <see langword="null"/> reference is allowed.
        /// </summary>
        /// <param name="classRef">The class name, optionally followed by <c>#</c> and a field name.</param>
        /// <exception cref="java.lang.SecurityException">The class is denied, or is not allowed.</exception>
        public void Check(string? classRef)
        {
            if (classRef is null)
                return;

            var hash = classRef.IndexOf('#');
            var className = hash >= 0 ? classRef.Substring(0, hash) : classRef;

            foreach (var pattern in _denylist)
                if (Matches(pattern, className))
                    throw new java.lang.SecurityException($"Class '{className}' rejected by the denylist (pattern '{pattern}').");

            foreach (var pattern in _allowlist)
                if (Matches(pattern, className))
                    return;

            throw new java.lang.SecurityException($"Class '{className}' rejected by the allowlist.");
        }

        static bool Matches(string pattern, string className)
        {
            return pattern.EndsWith('.') ? className.StartsWith(pattern, StringComparison.Ordinal) : className == pattern;
        }

        static string Append(string first, string second)
        {
            if (first.Length == 0)
                return second;
            if (second.Length == 0)
                return first;

            return first + "," + second;
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
