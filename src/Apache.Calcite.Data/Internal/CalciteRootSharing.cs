namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// How the sessions of a <see cref="CalciteDataSourceRoot"/> use its root schema.
    /// </summary>
    internal enum CalciteRootSharing
    {

        /// <summary>
        /// One session uses the root, which was built for it alone and is retired with it. A data source with
        /// <c>Pooling=false</c> builds its roots this way.
        /// </summary>
        Owned,

        /// <summary>
        /// Every session uses the root itself, so DDL on one connection is visible on the others. Planning and
        /// DDL are serialized by <see cref="CalciteDataSourceRoot.Lock"/>; execution is not guarded.
        /// </summary>
        Shared,

        /// <summary>
        /// Each session uses a root of its own, assembled from the same model and steps over the same schema
        /// objects, so DDL and the tables execution registers at the root stay with the connection.
        /// </summary>
        Isolated,

    }

}
