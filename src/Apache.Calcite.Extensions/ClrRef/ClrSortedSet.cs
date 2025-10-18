using System;
using System.Collections.Generic;

using java.util;

namespace Apache.Calcite.Extensions.ClrRef;

/// <summary>
/// Java wrapper for <see cref="ISet{T}"/>.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ClrSortedSet<T> : SortedSet
    where T : class
{

    readonly SortedSet<T> _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public ClrSortedSet(SortedSet<T> self)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
    }

    /// <inheritdoc />
    public override int size()
    {
        return _self.Count;
    }

    /// <inheritdoc />
    public override bool contains(object o)
    {
        return _self.Contains((T)o);
    }

    /// <inheritdoc />
    public override Iterator iterator()
    {
        return new ClrIterator<T>(_self.GetEnumerator());
    }

}
