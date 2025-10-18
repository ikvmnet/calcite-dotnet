using System;
using System.Collections.Generic;

using java.lang;
using java.util;
using java.util.function;

namespace Apache.Calcite.Extensions.ClrRef;

/// <summary>
/// Implementation of <see cref="ListIterator"/> that wraps a CLR list.
/// </summary>
/// <typeparam name="T"></typeparam>
class ClrListIterator<T> : Iterator, ListIterator
    where T : class
{

    readonly IList<T> _list;
    int _cursor; // index of next element to return
    int _lastRet = -1; // index of last element returned; -1 if no such

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="list"></param>
    /// <param name="index"></param>
    public ClrListIterator(IList<T> list, int index)
    {
        _list = list ?? throw new ArgumentNullException(nameof(list));
        _cursor = index;
    }

    /// <inheritdoc />
    public bool hasNext()
    {
        return _cursor != _list.Count;
    }

    /// <inheritdoc />
    public object next()
    {
        int i = _cursor;
        if (i >= _list.Count)
            throw new NoSuchElementException();

        _cursor = i + 1;
        return _list[_lastRet = i];
    }

    /// <inheritdoc />
    public void remove()
    {
        if (_lastRet < 0)
            throw new IllegalStateException();

        _list.RemoveAt(_lastRet);
        _cursor = _lastRet;
        _lastRet = -1;
    }

    /// <inheritdoc />
    public void forEachRemaining(Consumer consumer)
    {
        if (consumer is null)
            throw new NullPointerException();

        var size = _list.Count;
        int i = _cursor;
        if (i >= size)
            return;

        while (i != size)
            consumer.accept(_list[i++]);

        // update once at end of iteration to reduce heap write traffic
        _cursor = i;
        _lastRet = i - 1;
    }

    /// <inheritdoc />
    public bool hasPrevious()
    {
        return _cursor != 0;
    }

    /// <inheritdoc />
    public int nextIndex()
    {
        return _cursor;
    }

    /// <inheritdoc />
    public int previousIndex()
    {
        return _cursor - 1;
    }

    /// <inheritdoc />
    public object previous()
    {
        int i = _cursor - 1;
        if (i < 0)
            throw new NoSuchElementException();

        _cursor = i;
        return _list[_lastRet = i];
    }

    /// <inheritdoc />
    public void set(object? e)
    {
        if (_lastRet < 0)
            throw new IllegalStateException();

        _list[_lastRet] = (T)e!;
    }

    /// <inheritdoc />
    public void add(object? e)
    {
        int i = _cursor;
        _list.Insert(i, (T)e!);
        _cursor = i + 1;
        _lastRet = -1;
    }

}
