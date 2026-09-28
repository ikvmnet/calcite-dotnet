using System;
using System.Collections.Generic;

using org.apache.calcite.schema;

using Apache.Calcite.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Builds a <see cref="CalciteDataSource"/> from a connection string together with schema instances,
    /// root-schema configuration and type mappings that a connection string cannot express.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use the builder to register a schema the application constructs itself, for example one that wraps
    /// a client it already owns or an in-memory collection. Schemas and configuration steps are applied to
    /// the data source's root schema after the model, in the order they were added, and every connection
    /// the data source produces sees them.
    /// </para>
    /// <code>
    /// var dataSource = new CalciteDataSourceBuilder("Lex=MYSQL_ANSI")
    ///     .AddSchema("MEM", new MySchema())
    ///     .Build();
    ///
    /// await using var connection = await dataSource.OpenConnectionAsync();
    /// </code>
    /// <para>
    /// The data source returned by <see cref="Build"/> belongs to the caller, who disposes it. It is
    /// independent of the data sources the provider keeps for connections created from a connection string
    /// alone.
    /// </para>
    /// </remarks>
    public sealed class CalciteDataSourceBuilder
    {

        readonly List<Action<SchemaPlus>> _configure = [];
        readonly ClrTypeMapper _typeMapper = new();

        /// <summary>
        /// Gets the chain of type resolvers every connection from the built data source starts with.
        /// </summary>
        /// <remarks>
        /// Register here the mappings that belong to the data rather than to one caller, such as a domain type
        /// a schema produces or the .NET type a column should be read as. <see cref="Build"/> captures the
        /// chain as it stands; later changes do not affect a data source already built. Each connection starts
        /// from a copy, so a resolver added to <see cref="CalciteConnection.TypeMapper"/> affects that
        /// connection only.
        /// </remarks>
        public ClrTypeMapper TypeMapper => _typeMapper;

        /// <summary>
        /// Adds a type resolver ahead of every other in <see cref="TypeMapper"/>, so that it is consulted first.
        /// </summary>
        /// <param name="resolver">The resolver.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="resolver"/> is <see langword="null"/>.</exception>
        public CalciteDataSourceBuilder AddTypeResolver(IClrTypeResolver resolver)
        {
            ArgumentNullException.ThrowIfNull(resolver);

            _typeMapper.Prepend(resolver);
            return this;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteDataSourceBuilder"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string, or <see langword="null"/> for an empty one.
        /// Recognized keys are described on <see cref="CalciteConnectionStringBuilder"/>.</param>
        public CalciteDataSourceBuilder(string? connectionString = null)
        {
            ConnectionStringBuilder = new CalciteConnectionStringBuilder(connectionString);
        }

        /// <summary>
        /// Gets the connection string builder. <see cref="Build"/> copies the connection string as it stands
        /// at that moment.
        /// </summary>
        public CalciteConnectionStringBuilder ConnectionStringBuilder { get; }

        /// <summary>
        /// Gets the connection string as it currently stands.
        /// </summary>
        public string ConnectionString => ConnectionStringBuilder.ConnectionString;

        /// <summary>
        /// Adds a schema to the root schema of the data source being built.
        /// </summary>
        /// <param name="name">The name to register the schema under.</param>
        /// <param name="schema">The schema.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> or <paramref name="schema"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// The same instance is added to every root the data source builds: once normally, again after
        /// <see cref="CalciteDataSource.Clear"/>, and once per connection under <c>Pooling=false</c>. A schema
        /// that implements <see cref="IDisposable"/> is disposed each time a root holding it is released.
        /// </remarks>
        public CalciteDataSourceBuilder AddSchema(string name, Schema schema)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(schema);

            return ConfigureRootSchema(root => root.add(name, schema));
        }

        /// <summary>
        /// Registers a step to run over the root schema of the data source being built, after the model.
        /// </summary>
        /// <param name="configure">The step, given the root as Calcite's mutable <see cref="SchemaPlus"/>. It
        /// can add anything the root accepts: a schema that needs its parent, a table, a function, a view
        /// macro.</param>
        /// <returns>This builder.</returns>
        /// <remarks>
        /// The step runs each time a root is built: when the first connection opens, again after
        /// <see cref="CalciteDataSource.Clear"/>, and once per connection under <c>Pooling=false</c>. This is
        /// the only place the root is exposed as a <see cref="SchemaPlus"/>; a connection exposes it as a
        /// read-only <see cref="Schema"/> through <see cref="CalciteConnection.RootSchema"/>, because the root
        /// is shared by every connection of the data source.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
        public CalciteDataSourceBuilder ConfigureRootSchema(Action<SchemaPlus> configure)
        {
            ArgumentNullException.ThrowIfNull(configure);

            _configure.Add(configure);
            return this;
        }

        /// <summary>
        /// Builds the data source from the connection string, schemas, steps and type mappings registered so
        /// far.
        /// </summary>
        /// <returns>A new data source, which the caller disposes.</returns>
        /// <exception cref="ArgumentException"><see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/>
        /// is not positive or exceeds <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>.</exception>
        /// <remarks>
        /// The model is not read here; the root schema is built when the first connection opens. The builder
        /// can be used again to build further data sources.
        /// </remarks>
        public CalciteDataSource Build()
        {
            return new CalciteDataSource(new CalciteConnectionStringBuilder(ConnectionString), _configure.ToArray(), typeMapper: _typeMapper);
        }

    }

}
