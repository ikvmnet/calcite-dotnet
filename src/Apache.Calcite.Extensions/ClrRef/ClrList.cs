using System;
using System.Collections.Generic;

using java.util;

namespace Apache.Calcite.Extensions.ClrRef;

public partial class ClrList<T> : AbstractList
    where T : class
{

    readonly IList<T> _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="self"></param>
    public ClrList(IList<T> self)
    {
        _self = self ?? throw new ArgumentNullException(nameof(self));
    }

    /// <inheritdoc />
    public override bool add(object e)
    {
        _self.Add((T)e);
        return true;
    }

    /// <inheritdoc />
    public override void add(int index, object element)
    {
        _self.Insert(index, (T)element);
    }

    /// <inheritdoc />
    public override bool addAll(Collection c)
    {
        var v = false;

        foreach (var i in c.AsRef<T>())
        {
            _self.Add(i);
            v = true;
        }

        return v;
    }

    /// <inheritdoc />
    public override bool addAll(int index, Collection c)
    {
        var v = false;

        foreach (var i in c.AsRef<T>())
        {
            _self.Insert(index++, i);
            v = true;
        }

        return v;
    }

    /// <inheritdoc />
    public override void clear()
    {
        _self.Clear();
    }

    /// <inheritdoc />
    public override bool contains(object o)
    {
        return _self.Contains((T)o);
    }

    ///// <inheritdoc />
    //public  bool contdainsAll(Collection c)
    //{
    //    foreach (var i in c.AsRef<T>())
    //        if (_self.Contains(i) == false)
    //            return false;

    //    return true;
    //}

    ///// <inheritdoc />
    //public void forEach(Consumer action)
    //{
    //    foreach (var i in _self)
    //        action.accept(i);
    //}

    /// <inheritdoc />
    public override object get(int index)
    {
        return _self[index];
    }

    /// <inheritdoc />
    public override int indexOf(object o)
    {
        return _self.IndexOf((T)o);
    }

    /// <inheritdoc />
    public override bool isEmpty()
    {
        return _self.Count == 0;
    }

    /// <inheritdoc />
    public override bool remove(object o)
    {
        return _self.Remove((T)o);
    }

    /// <inheritdoc />
    public override object remove(int index)
    {
        var o = _self[index];
        _self.RemoveAt(index);
        return o;
    }

    /// <inheritdoc />
    public override object set(int index, object element)
    {
        var v = _self[index];
        _self[index] = (T)element;
        return v;
    }

    /// <inheritdoc />
    public override int size()
    {
        return _self.Count;
    }

    /// <inheritdoc />
    public override void sort(Comparator c)
    {
        if (_self is List<T> l)
            l.Sort(c.AsRef<T, T, RefIdInfo<T>>());
        else
            base.sort(c);
    }

}
