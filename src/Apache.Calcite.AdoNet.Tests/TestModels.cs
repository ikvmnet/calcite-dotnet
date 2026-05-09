namespace Apache.Calcite.AdoNet.Tests
{

    /// <summary>
    /// Shared test fixtures and connection-string constants used across the test suite.
    /// </summary>
    internal static class TestModels
    {

        /// <summary>
        /// A minimal inline JSON model that registers a single empty schema named <c>adhoc</c>.
        /// </summary>
        public const string InlineEmptyModelJson =
            "inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}";

        /// <summary>
        /// A connection string referencing <see cref="InlineEmptyModelJson"/>.
        /// </summary>
        public const string InlineEmptyModelConnectionString =
            "Model=inline:{\"version\":\"1.0\",\"defaultSchema\":\"adhoc\",\"schemas\":[{\"name\":\"adhoc\"}]}";

    }

}
