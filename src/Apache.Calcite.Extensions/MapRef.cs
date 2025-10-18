using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions;

/// <summary>
/// Wraps a Java <see cref="Map"/> type in a CLR compatible accessor.
/// </summary>
/// <typeparam name="TKey"></typeparam>
/// <typeparam name="TKeyRef"></typeparam>
/// <typeparam name="TKeyInfo"></typeparam>
/// <typeparam name="TValue"></typeparam>
/// <typeparam name="TValueRef"></typeparam>
/// <typeparam name="TValueInfo"></typeparam>
public readonly struct MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo> :
    IDictionary<TKeyRef, TValueRef>,
    IReadOnlyDictionary<TKeyRef, TValueRef>,
    IRef<Map, MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo>>
    where TKey : class
    where TKeyInfo : IRefInfo<TKey, TKeyRef>
    where TValue : class
    where TValueInfo : IRefInfo<TValue, TValueRef>
{

    /// <inheritdoc/>
    public static MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo> Create(Map? value) => new MapRef<TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo>(value);

    /// <summary>
    /// Enumerator of a <see cref="MapRef{TKey, TKeyRef, TKeyInfo, TValue, TValueRef, TValueInfo}"/>.
    /// </summary>
    public struct Enumerator : IEnumerator<KeyValuePair<TKeyRef, TValueRef>>
    {

        readonly Map? _self;
        Iterator? _keys;
        TKey? _current;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="self"></param>
        public Enumerator(Map? self)
        {
            _self = self;
            Reset();
        }

        /// <inheritdoc />
        [MemberNotNullWhen(false, nameof(_self))]
        [MemberNotNullWhen(false, nameof(_keys))]
        public readonly bool IsNull => _self == null || _keys == null;

        /// <summary>
        /// Throws if the reference is null.
        /// </summary>
        /// <exception cref="NullReferenceException"></exception>
        [MemberNotNull(nameof(_self))]
        [MemberNotNull(nameof(_keys))]
        readonly void ThrowOnNull()
        {
            if (IsNull)
                throw new NullReferenceException();
        }

        /// <inheritdoc />
        public readonly KeyValuePair<TKeyRef, TValueRef> Current
        {
            get
            {
                ThrowOnNull();

                if (_current == null)
                    throw new InvalidOperationException();

                return new KeyValuePair<TKeyRef, TValueRef>(TKeyInfo.ToCast(_current), TValueInfo.ToCast((TValue)_self.get(_current)));
            }
        }

        /// <inheritdoc />
        readonly object IEnumerator.Current => Current;

        /// <inheritdoc />
        public bool MoveNext()
        {
            ThrowOnNull();

            if (_keys.hasNext())
            {
                _current = (TKey?)_keys.next();
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <inheritdoc />
        public void Reset()
        {
            _keys = _self?.keySet().iterator();
            _current = null;
        }

        /// <inheritdoc />
        public void Dispose()
        {

        }

    }

    readonly Map? _self;

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="map"></param>
    public MapRef(Map? map)
    {
        _self = map;
    }

    /// <inheritdoc />
    public readonly Map? Underlying => _self;

    /// <inheritdoc />
    [MemberNotNullWhen(false, nameof(_self))]
    public readonly bool IsNull => _self == null;

    /// <summary>
    /// Throws if the reference is null.
    /// </summary>
    /// <exception cref="NullReferenceException"></exception>
    [MemberNotNull(nameof(_self))]
    readonly void ThrowOnNull()
    {
        if (IsNull)
            throw new NullReferenceException();
    }

    /// <inheritdoc />
    public TValueRef this[TKeyRef key]
    {
        get
        {
            ThrowOnNull();
            return TValueInfo.ToCast((TValue)_self.get(TKeyInfo.ToBind(key)));
        }
        set
        {
            ThrowOnNull();
            _self.put(TKeyInfo.ToBind(key), TValueInfo.ToBind(value));
        }
    }

    /// <inheritdoc/>
    public SetRef<TKey, TKeyRef, TKeyInfo> Keys
    {
        get
        {
            ThrowOnNull();
            return _self.keySet().AsRef<TKey, TKeyRef, TKeyInfo>();
        }
    }

    /// <inheritdoc/>
    public CollectionRef<TValue, TValueRef, TValueInfo> Values
    {
        get
        {
            ThrowOnNull();
            return _self.values().AsRef<TValue, TValueRef, TValueInfo>();
        }
    }

    /// <inheritdoc/>
    public int Count
    {
        get
        {
            ThrowOnNull();
            return _self.size();
        }
    }

    /// <inheritdoc/>
    public bool IsReadOnly => false;

    /// <inheritdoc/>
    ICollection<TKeyRef> IDictionary<TKeyRef, TValueRef>.Keys => Keys;

    /// <inheritdoc/>
    ICollection<TValueRef> IDictionary<TKeyRef, TValueRef>.Values => Values;

    /// <inheritdoc/>
    IEnumerable<TKeyRef> IReadOnlyDictionary<TKeyRef, TValueRef>.Keys => Keys;

    /// <inheritdoc/>
    IEnumerable<TValueRef> IReadOnlyDictionary<TKeyRef, TValueRef>.Values => Values;

    /// <inheritdoc/>
    public void Add(TKeyRef key, TValueRef value)
    {
        ThrowOnNull();
        _self.putIfAbsent(TKeyInfo.ToBind(key), TValueInfo.ToBind(value));
    }

    /// <inheritdoc/>
    public void Add(KeyValuePair<TKeyRef, TValueRef> item)
    {
        ThrowOnNull();
        _self.putIfAbsent(TKeyInfo.ToBind(item.Key), TValueInfo.ToBind(item.Value));
    }

    /// <inheritdoc/>
    public void Clear()
    {
        ThrowOnNull();
        _self.clear();
    }

    /// <inheritdoc/>
    public bool Contains(KeyValuePair<TKeyRef, TValueRef> item)
    {
        ThrowOnNull();
        return _self.containsKey(TKeyInfo.ToBind(item.Key)) && _self.containsValue(TValueInfo.ToBind(item.Value));
    }

    /// <inheritdoc/>
    public bool ContainsKey(TKeyRef key)
    {
        ThrowOnNull();
        return _self.containsKey(TKeyInfo.ToBind(key));
    }

    /// <inheritdoc/>
    public void CopyTo(KeyValuePair<TKeyRef, TValueRef>[] array, int arrayIndex)
    {
        ThrowOnNull();

        var i = 0;
        foreach (var e in this)
            array[arrayIndex + i++] = new KeyValuePair<TKeyRef, TValueRef>(e.Key, e.Value);
    }

    /// <inheritdoc/>
    public Enumerator GetEnumerator()
    {
        ThrowOnNull();
        return new Enumerator(_self);
    }

    /// <inheritdoc/>
    public bool Remove(TKeyRef key)
    {
        ThrowOnNull();
        return _self.remove(key) != null;
    }

    /// <inheritdoc/>
    public bool Remove(KeyValuePair<TKeyRef, TValueRef> item)
    {
        ThrowOnNull();
        return _self.remove(TKeyInfo.ToBind(item.Key), TValueInfo.ToBind(item.Value));
    }

    /// <inheritdoc/>
    public bool TryGetValue(TKeyRef key, [MaybeNullWhen(false)] out TValueRef value)
    {
        ThrowOnNull();

        if (_self.containsKey(TKeyInfo.ToBind(key)))
        {
            value = TValueInfo.ToCast((TValue)_self.get(TKeyInfo.ToBind(key)));
            return true;
        }
        else
        {
            value = default;
            return false;
        }
    }

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <inheritdoc/>
    IEnumerator<KeyValuePair<TKeyRef, TValueRef>> IEnumerable<KeyValuePair<TKeyRef, TValueRef>>.GetEnumerator()
    {
        return GetEnumerator();
    }

}
