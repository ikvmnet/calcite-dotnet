using System.Collections.Generic;

using java.lang;
using java.util;
using java.util.function;

namespace Apache.Calcite.Extensions.ClrRef;

public class ClrIterator<T> : Iterator
{

    readonly IEnumerator<T?> _self;
    bool _hasNext;
    T? _next;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    /// <exception cref="System.ArgumentNullException"></exception>
    public ClrIterator(IEnumerator<T?> self)
    {
        _self = self ?? throw new System.ArgumentNullException(nameof(self));
        _hasNext = self.MoveNext();
        _next = self.Current;
    }

    /// <inheritdoc />
    public bool hasNext()
    {
        return _hasNext;
    }

    /// <inheritdoc />
    public object? next()
    {
        if (_hasNext == false)
            throw new NoSuchElementException();

        var n = _next;
        if (_hasNext = _self.MoveNext())
            _next = _self.Current;

        return n;
    }

    /// <inheritdoc />
    public void forEachRemaining(Consumer action)
    {
        while (_self.MoveNext())
            action.accept(_self.Current);
    }

    /// <inheritdoc />
    public void remove()
    {
        throw new UnsupportedOperationException();
    }

}