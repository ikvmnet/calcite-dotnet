namespace Apache.Calcite.Extensions
{

    /// <summary>
    /// Describes a structure that wraps a non-generic type with generic methods.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IRef<out T>
        where T : class
    {

        /// <summary>
        /// Gets the real underlying instance.
        /// </summary>
        T? Underlying { get; }

        /// <summary>
        /// Returns <c>true</c> if the given typed value is null.
        /// </summary>
        bool IsNull { get; }

    }

    /// <summary>
    /// Describes a structure that wraps a non-generic type with generic methods.
    /// </summary>
    /// <typeparam name="TSelf"></typeparam>
    /// <typeparam name="T"></typeparam>
    public interface IRef<T, TSelf> :
        IRef<T>
        where T : class
        where TSelf : struct, IRef<T, TSelf>
    {

        /// <summary>
        /// Returns <c>true</c> if the underlying referenced instances are equal.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static virtual bool operator ==(TSelf a, TSelf b)
        {
            return ReferenceEquals(a.Underlying, b.Underlying);
        }

        /// <summary>
        /// Returns <c>true</c> if the underlying referenced instances are not equal.
        /// </summary>
        /// <param name="a"></param>
        /// <param name="b"></param>
        /// <returns></returns>
        public static virtual bool operator !=(TSelf a, TSelf b)
        {
            return ReferenceEquals(a.Underlying, b.Underlying) == false;
        }

        /// <summary>
        /// Converts from a reference to the underlying instance.
        /// </summary>
        /// <param name="value"></param>
        public static virtual implicit operator T?(TSelf value)
        {
            return value.Underlying;
        }

        /// <summary>
        /// Converts from an underlying instance to a reference.
        /// </summary>
        /// <param name="value"></param>
        public static virtual implicit operator TSelf(T? value)
        {
            return TSelf.Create(value);
        }

        /// <summary>
        /// Creates a new instance of the reference from the underlying type.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static abstract TSelf Create(T? value);

    }

}
