using Apache.Calcite.Extensions;

namespace org.apache.calcite.util;

/// <summary>
/// Implements <see cref="IRefInfo{TBind, TCast}"/> that supports casting between the two according to their implicit operators.
/// </summary>
/// <typeparam name="TBind"></typeparam>
/// <typeparam name="TCast"></typeparam>
public readonly struct RefBinder<TBind, TCast> : IRefInfo<TBind, TCast>
    where TBind : class
    where TCast : struct, IRef<TBind, TCast>
{

    /// <inheritdoc/>
    public static TCast ToCast(TBind? value) => value;

    /// <inheritdoc/>
    public static TBind? ToBind(TCast value) => value;

}
