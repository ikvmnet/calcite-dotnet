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
    /// the data source produces sees them. <see cref="UseSharedRootSchema"/> makes the connections share the
    /// root itself, so that what DDL creates on one is visible on the others.
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
        bool _sharedRootSchema;

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
        /// The same instance is added to every root the data source assembles, so every connection shares it. A
        /// schema that implements <see cref="IDisposable"/> is disposed when the data source's roots are
        /// released: after <see cref="CalciteDataSource.Clear"/> or disposal, or with the connection under
        /// <c>Pooling=false</c>.
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
        /// <para>
        /// The step runs for every root the data source assembles. Each connection plans against a root of its
        /// own, kept for the next connection while no DDL has touched it, so the step runs for the first
        /// connection and again whenever a new root is needed; with a shared root, or under
        /// <c>Pooling=false</c>, there is one root per data source or per connection. A view macro the step adds
        /// is analysed against the root it was given, as one a model declares is.
        /// </para>
        /// <para>
        /// An instance the step adds to every root is shared by the connections. One it creates each time
        /// belongs to that root: a schema that implements <see cref="IDisposable"/> is disposed with the root,
        /// except the first root's, which is disposed with the data source's roots.
        /// </para>
        /// <para>
        /// This is the only place a root is exposed as a <see cref="SchemaPlus"/>; a connection exposes it as a
        /// read-only <see cref="Schema"/> through <see cref="CalciteConnection.RootSchema"/>.
        /// </para>
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="configure"/> is <see langword="null"/>.</exception>
        public CalciteDataSourceBuilder ConfigureRootSchema(Action<SchemaPlus> configure)
        {
            ArgumentNullException.ThrowIfNull(configure);

            _configure.Add(configure);
            return this;
        }

        /// <summary>
        /// Makes every connection of the built data source plan against one root schema, so that a table,
        /// view or schema created by DDL on one connection is visible on the others.
        /// </summary>
        /// <returns>This builder.</returns>
        /// <remarks>
        /// <para>
        /// By default each connection plans against a root of its own over the data source's schemas, and what
        /// DDL creates stays with the connection. With a shared root, what DDL creates lasts as long as the
        /// data source, until <see cref="CalciteDataSource.Clear"/> or disposal, whether or not a connection is
        /// open.
        /// </para>
        /// <para>
        /// A shared root is not safe to use from several connections at once. Statements plan under a shared
        /// lock and DDL runs under an exclusive one, but execution is not guarded: a recursive query registers
        /// its working table at the root under the query's own name while it runs and removes it when it is
        /// closed, so two such queries running at once on different connections read each other's rows and
        /// return wrong results without failing. Use one for a single caller that needs
        /// DDL to outlive a connection, such as a test fixture.
        /// </para>
        /// <para>
        /// Cannot be combined with <c>Pooling=false</c>, under which every connection builds a root of its own.
        /// </para>
        /// </remarks>
        public CalciteDataSourceBuilder UseSharedRootSchema()
        {
            _sharedRootSchema = true;
            return this;
        }

        /// <summary>
        /// Builds the data source from the connection string, schemas, steps and type mappings registered so
        /// far.
        /// </summary>
        /// <returns>A new data source, which the caller disposes.</returns>
        /// <exception cref="ArgumentException"><see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/>
        /// is not positive or exceeds <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>, or
        /// <see cref="UseSharedRootSchema"/> was called and the connection string sets <c>Pooling=false</c>.</exception>
        /// <remarks>
        /// The model is not read here; the root schema is built when the first connection opens. The builder
        /// can be used again to build further data sources.
        /// </remarks>
        public CalciteDataSource Build()
        {
            return new CalciteDataSource(new CalciteConnectionStringBuilder(ConnectionString), _configure.ToArray(), typeMapper: _typeMapper, sharedRoot: _sharedRootSchema);
        }

    }

}
