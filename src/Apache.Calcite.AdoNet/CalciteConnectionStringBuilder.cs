using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.AdoNet
{

    /// <summary>
    /// Strongly typed connection string builder for the Apache Calcite ADO.NET provider.
    /// </summary>
    /// <remarks>
    /// Property names mirror the Calcite JDBC driver connection properties where practical, while
    /// surfacing them through .NET conventions. Unknown keys are preserved so the underlying engine
    /// can read provider-specific options.
    /// </remarks>
    public sealed class CalciteConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Connection string key for the Calcite model file URI or inline JSON model.
        /// </summary>
        public const string ModelKey = "Model";

        /// <summary>
        /// Connection string key for the default schema.
        /// </summary>
        public const string SchemaKey = "Schema";

        /// <summary>
        /// Connection string key for whether identifiers are matched case-sensitively.
        /// </summary>
        public const string CaseSensitiveKey = "CaseSensitive";

        /// <summary>
        /// Connection string key for the SQL conformance level.
        /// </summary>
        public const string ConformanceKey = "Conformance";

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnectionStringBuilder"/> class.
        /// </summary>
        public CalciteConnectionStringBuilder()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnectionStringBuilder"/> class with the specified connection string.
        /// </summary>
        /// <param name="connectionString"></param>
        public CalciteConnectionStringBuilder(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the Calcite model file URI or inline JSON model.
        /// </summary>
        public string? Model
        {
            get => TryGetString(ModelKey);
            set => SetOrRemove(ModelKey, value);
        }

        /// <summary>
        /// Gets or sets the default schema name.
        /// </summary>
        public string? Schema
        {
            get => TryGetString(SchemaKey);
            set => SetOrRemove(SchemaKey, value);
        }

        /// <summary>
        /// Gets or sets a value indicating whether identifiers are matched case-sensitively.
        /// </summary>
        public bool? CaseSensitive
        {
            get => TryGetBool(CaseSensitiveKey);
            set
            {
                if (value is null)
                    Remove(CaseSensitiveKey);
                else
                    this[CaseSensitiveKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets the SQL conformance level (e.g. <c>DEFAULT</c>, <c>STRICT_2003</c>, <c>PRAGMATIC_2003</c>).
        /// </summary>
        public string? Conformance
        {
            get => TryGetString(ConformanceKey);
            set => SetOrRemove(ConformanceKey, value);
        }

        /// <summary>
        /// Returns the keys present in this connection string as an enumerable.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> EnumerateKeys()
        {
            foreach (var key in Keys)
                if (key is string s)
                    yield return s;
        }

        string? TryGetString(string key)
        {
            return TryGetValue(key, out var v) ? v?.ToString() : null;
        }

        bool? TryGetBool(string key)
        {
            if (TryGetValue(key, out var v) == false || v is null)
                return null;

            if (v is bool b)
                return b;

            return bool.TryParse(v.ToString(), out var parsed) ? parsed : null;
        }

        void SetOrRemove(string key, string? value)
        {
            if (string.IsNullOrEmpty(value))
                Remove(key);
            else
                this[key] = value!;
        }

    }

}
