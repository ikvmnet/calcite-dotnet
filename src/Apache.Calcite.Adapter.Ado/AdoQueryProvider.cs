using org.apache.calcite.linq4j;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Implementationo f <see cref="QueryProvider"/> that talks to an ADO.NET database.
    /// </summary>
    public class AdoQueryProvider : QueryProviderImpl
    {

        /// <summary>
        /// Gets the default singleton instance.
        /// </summary>
        public static readonly AdoQueryProvider Instance = new AdoQueryProvider();

        /// <inheritdoc />
        public override Enumerator? executeQuery(Queryable value)
        {
            return null;
        }

    }

}
