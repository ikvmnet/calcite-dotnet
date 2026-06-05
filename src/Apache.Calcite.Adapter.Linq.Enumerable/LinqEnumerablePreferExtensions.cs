using org.apache.calcite.adapter.enumerable;

namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    public static class LinqEnumerablePreferExtensions
    {

        public static JavaRowFormat Prefer(this LinqEnumerablePrefer super, JavaRowFormat format)
        {
            switch (super)
            {
                case LinqEnumerablePrefer.Custom:
                    return JavaRowFormat.CUSTOM;
                case LinqEnumerablePrefer.Array:
                    return JavaRowFormat.ARRAY;
                default:
                    return format;
            }
        }

        public static LinqEnumerablePrefer Prefer(this JavaRowFormat format)
        {
            switch ((JavaRowFormat.__Enum) format.ordinal())
            {
                case JavaRowFormat.__Enum.ARRAY:
                    return LinqEnumerablePrefer.Array;
                default:
                    return LinqEnumerablePrefer.Custom;
            }
        }

    }

}
