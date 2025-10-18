using System;
using System.Collections.Generic;

using java.util;

namespace Apache.Calcite.Extensions.ClrRef;

/// <summary>
/// Java wrapper for <see cref="ISet{T}"/>.
/// </summary>
/// <typeparam name="T"></typeparam>
public class ClrSet<T> : AbstractSet
    where T : class
{

    readonly ISet<T> _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public ClrSet(ISet<T> self)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
    }

    /// <inheritdoc />
    public override int size()
    {
        return _self.Count;
    }

    /// <inheritdoc />
    public override bool add(object e)
    {
        return _self.Add((T)e);
    }

    /// <inheritdoc />
    public override bool remove(object o)
    {
        return _self.Remove((T)o);
    }

    /// <inheritdoc />
    public override bool contains(object o)
    {
        return _self.Contains((T)o);
    }

    /// <inheritdoc />
    public override void clear()
    {
        _self.Clear();
    }

    /// <inheritdoc />
    public override Iterator iterator()
    {
        return new ClrIterator<T>(_self.GetEnumerator());
    }

}
