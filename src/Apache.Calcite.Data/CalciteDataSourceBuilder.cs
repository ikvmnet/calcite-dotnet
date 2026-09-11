using System;
using System.Collections.Generic;

using Apache.Calcite.Data.Types;

using org.apache.calcite.schema;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Builds a <see cref="CalciteDataSource"/> from a connection string and whatever a connection string
    /// cannot carry.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A connection string can name a model, and a model can name a schema factory, and that is as far as
    /// text goes. A schema an application constructed itself — around a client it already owns, a token
    /// cache, an in-memory collection — has to be handed over as an object, and this is where. What is
    /// registered here is applied to the data source's root after the model, in the order it was added,
    /// and every connection the data source opens sees it.
    /// </para>
    /// <code>
    /// var dataSource = new CalciteDataSourceBuilder("Lex=MYSQL_ANSI")
    ///     .AddSchema("MEM", new MySchema())
    ///     .Build();
    ///
    /// await using var connection = await dataSource.OpenConnectionAsync();
    /// </code>
    /// <para>
    /// A data source built this way is the caller's: it is not shared with connections opened by connection
    /// string, and the caller disposes it.
    /// </para>
    /// </remarks>
    public sealed class CalciteDataSourceBuilder
    {

        readonly List<Action<SchemaPlus>> _configure = [];

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
        /// Gets the connection string builder, which can be changed until <see cref="Build"/> is called.
        /// </summary>
        public CalciteConnectionStringBuilder ConnectionStringBuilder { get; }

        /// <summary>
        /// Gets the connection string as it currently stands.
        /// </summary>
        public string ConnectionString => ConnectionStringBuilder.ConnectionString;

        /// <summary>
        /// Gets the CLR type mapping every connection of the data source starts from.
        /// </summary>
        /// <remarks>
        /// A resolver registered here is the whole data source's, which is where an application's own
        /// types belong: they do not change from one connection to the next. A connection takes a copy of
        /// this chain when it is created, so a resolver a connection adds for itself stays its own.
        /// </remarks>
        public ClrTypeMapper TypeMapper { get; } = new();

        /// <summary>
        /// Adds a schema to the root of the data source being built.
        /// </summary>
        /// <param name="name">The name to register the schema under.</param>
        /// <param name="schema">The schema.</param>
        /// <returns>This builder.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="name"/> or <paramref name="schema"/> is <see langword="null"/>.</exception>
        public CalciteDataSourceBuilder AddSchema(string name, Schema schema)
        {
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(schema);

            return ConfigureRootSchema(root => root.add(name, schema));
        }

        /// <summary>
        /// Registers a step to run over the root of the data source being built, after the model.
        /// </summary>
        /// <param name="configure">The step, given the root as Calcite's mutable <see cref="SchemaPlus"/>.
        /// Anything the root accepts can be added: a schema that needs its parent, a table, a function, a
        /// view macro.</param>
        /// <returns>This builder.</returns>
        /// <remarks>
        /// The step runs once per root — once for the data source, or once per connection under
        /// <c>Pooling=false</c> — and is the one place a caller meets the root as a <see cref="SchemaPlus"/>.
        /// A connection sees the root as a <see cref="Schema"/>, the read interface, because a change made
        /// through one connection would reach every connection of the data source.
        /// </remarks>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="configure"/> is <see langword="null"/>.</exception>
        public CalciteDataSourceBuilder ConfigureRootSchema(Action<SchemaPlus> configure)
        {
            ArgumentNullException.ThrowIfNull(configure);

            _configure.Add(configure);
            return this;
        }

        /// <summary>
        /// Builds the data source.
        /// </summary>
        /// <returns>A data source that is the caller's to dispose.</returns>
        public CalciteDataSource Build()
        {
            return new CalciteDataSource(new CalciteConnectionStringBuilder(ConnectionString), _configure.ToArray(), typeMapper: new ClrTypeMapper(TypeMapper));
        }

    }

}
