namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    public enum LinqEnumerablePrefer
    {

        /// <summary>
        /// Records should be represented as arrays of objects, with one array element per column. This is the default.
        /// </summary>
        Array,

        /// <summary>
        /// Records should be represented as objects, with one value per record.
        /// </summary>
        Custom,

        /// <summary>
        /// Consumer has no preference regarding the representation of records.
        /// </summary>
        Any,

    }

}
