using System.Diagnostics.CodeAnalysis;

namespace Apache.Calcite.Extensions;

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
/// <typeparam name="TRef"></typeparam>
/// <typeparam name="T"></typeparam>
public interface IRef<T, TRef> :
    IRef<T>
    where T : class
    where TRef : IRef<T, TRef>
{

    /// <summary>
    /// Returns <c>true</c> if the underlying referenced instances are equal.
    /// </summary>
    /// <param name="a"></param>
    /// <param name="b"></param>
    /// <returns></returns>
    public static virtual bool operator ==(TRef a, TRef b)
    {
        return ReferenceEquals(a.Underlying, b.Underlying);
    }

    /// <summary>
    /// Returns <c>true</c> if the underlying referenced instances are not equal.
    /// </summary>
    /// <param name="a"></param>
    /// <param name="b"></param>
    /// <returns></returns>
    public static virtual bool operator !=(TRef a, TRef b)
    {
        return ReferenceEquals(a.Underlying, b.Underlying) == false;
    }

    /// <summary>
    /// Converts from a reference to the underlying instance.
    /// </summary>
    /// <param name="value"></param>
    public static virtual implicit operator T?(TRef value)
    {
        return value.Underlying;
    }

    /// <summary>
    /// Converts from an underlying instance to a reference.
    /// </summary>
    /// <param name="value"></param>
    public static virtual explicit operator TRef(T? value)
    {
        return TRef.Create(value);
    }

    /// <summary>
    /// Creates a new instance of the reference from the underlying type.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public static abstract TRef Create(T? value);

}
