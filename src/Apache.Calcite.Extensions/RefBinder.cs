namespace Apache.Calcite.Extensions;

/// <summary>
/// Implements <see cref="IRefInfo{TBind, TCast}"/> that supports casting between the two according to their implicit operators.
/// </summary>
/// <typeparam name="TBind"></typeparam>
/// <typeparam name="TCast"></typeparam>
public readonly struct RefBinder<TBind, TCast> : IRefInfo<TBind, TCast>
    where TBind : class
    where TCast : struct, IRef<TBind, TCast>
{

    /// <inheritdoc />
    public static TBind? Null => default;

    /// <inheritdoc />
    public static TCast RefNull => default;

    /// <inheritdoc/>
    public static bool IsNull(TCast value) => value.IsNull;

    /// <inheritdoc/>
    public static TBind? ToBind(TCast value) => value;

    /// <inheritdoc/>
    public static TCast ToCast(TBind? value) => (TCast)value;

    /// <inheritdoc/>
    public static bool IsNull(TBind? value) => value is null;

}
