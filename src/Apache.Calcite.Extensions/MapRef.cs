using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions
{

    /// <summary>
    /// Wrapper around a <see cref="Map"/> that retypes the values.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TKeyRef"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    public readonly struct MapRef<TKey, TKeyRef, TValue, TValueRef> :
        IDictionary<TKeyRef, TValueRef>,
        IReadOnlyDictionary<TKeyRef, TValueRef>,
        IRef<Map, MapRef<TKey, TKeyRef, TValue, TValueRef>>
        where TKey : class
        where TKeyRef : struct, IRef<TKey, TKeyRef>
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {

        /// <inheritdoc/>
        public static MapRef<TKey, TKeyRef, TValue, TValueRef> Create(Map? value) => new MapRef<TKey, TKeyRef, TValue, TValueRef>(value);

        /// <summary>
        /// Enumerator of a <see cref="MapRef{TKey, TKeyRef, TValue, TValueRef}"/>.
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

                    return new KeyValuePair<TKeyRef, TValueRef>(_current, (TValue?)_self.get(_current));
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
                return (TValue?)_self.get((TKey?)key);
            }
            set
            {
                ThrowOnNull();
                _self.put((TKey?)key, (TValue?)value);
            }
        }

        /// <inheritdoc/>
        public SetRef<TKey, TKeyRef> Keys
        {
            get
            {
                ThrowOnNull();
                return _self.keySet().AsRef<TKey, TKeyRef>();
            }
        }

        /// <inheritdoc/>
        public CollectionRef<TValue, TValueRef> Values
        {
            get
            {
                ThrowOnNull();
                return _self.values().AsRef<TValue, TValueRef>();
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
            _self.putIfAbsent((TKey?)key, (TValue?)value);
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<TKeyRef, TValueRef> item)
        {
            ThrowOnNull();
            _self.putIfAbsent((TKey?)item.Key, (TValue?)item.Value);
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
            return _self.containsKey((TKey?)item.Key) && _self.containsValue((TValue?)item.Value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(TKeyRef key)
        {
            ThrowOnNull();
            return _self.containsKey((TKey?)key);
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
            return _self.remove(item.Key, (TValue?)item.Value);
        }

        /// <inheritdoc/>
        public bool TryGetValue(TKeyRef key, [MaybeNullWhen(false)] out TValueRef value)
        {
            ThrowOnNull();

            if (_self.containsKey((TKey?)key))
            {
                value = (TValue?)_self.get((TKey?)key);
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

    /// <summary>
    /// Wrapper around a <see cref="Map"/> that retypes the values.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public readonly struct MapRef<TKey, TValue> :
        IDictionary<TKey?, TValue?>,
        IReadOnlyDictionary<TKey?, TValue?>,
        IRef<Map, MapRef<TKey, TValue>>
        where TKey : class
        where TValue : class
    {

        /// <inheritdoc/>
        public static MapRef<TKey, TValue> Create(Map? value) => new MapRef<TKey, TValue>(value);

        /// <summary>
        /// Enumerator of a <see cref="MapRef{TKey, TValue}"/>.
        /// </summary>
        public struct Enumerator : IEnumerator<KeyValuePair<TKey?, TValue?>>
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
            public readonly KeyValuePair<TKey?, TValue?> Current
            {
                get
                {
                    ThrowOnNull();

                    if (_current == null)
                        throw new InvalidOperationException();

                    return new KeyValuePair<TKey?, TValue?>(_current, (TValue?)_self.get(_current));
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
        [MemberNotNullWhen(false, nameof(_self))]
        public readonly bool IsNull => _self == null;

        /// <inheritdoc />
        public readonly Map? Underlying => _self;

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
        public TValue? this[TKey? key]
        {
            get
            {
                ThrowOnNull();
                return (TValue?)_self.get((TKey?)key);
            }
            set
            {
                ThrowOnNull();
                _self.put((TKey?)key, (TValue?)value);
            }
        }

        /// <inheritdoc/>
        public SetRef<TKey> Keys
        {
            get
            {
                ThrowOnNull();
                return _self.keySet().AsRef<TKey>();
            }
        }

        /// <inheritdoc/>
        public CollectionRef<TValue> Values
        {
            get
            {
                ThrowOnNull();
                return _self.values().AsRef<TValue>();
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
        ICollection<TKey?> IDictionary<TKey?, TValue?>.Keys => Keys;

        /// <inheritdoc/>
        ICollection<TValue?> IDictionary<TKey?, TValue?>.Values => Values;

        /// <inheritdoc/>
        IEnumerable<TKey?> IReadOnlyDictionary<TKey?, TValue?>.Keys => Keys;

        /// <inheritdoc/>
        IEnumerable<TValue?> IReadOnlyDictionary<TKey?, TValue?>.Values => Values;

        /// <inheritdoc/>
        public void Add(TKey? key, TValue? value)
        {
            ThrowOnNull();
            _self.putIfAbsent((TKey?)key, (TValue?)value);
        }

        /// <inheritdoc/>
        public void Add(KeyValuePair<TKey?, TValue?> item)
        {
            ThrowOnNull();
            _self.putIfAbsent((TKey?)item.Key, (TValue?)item.Value);
        }

        /// <inheritdoc/>
        public void Clear()
        {
            ThrowOnNull();
            _self.clear();
        }

        /// <inheritdoc/>
        public bool Contains(KeyValuePair<TKey?, TValue?> item)
        {
            ThrowOnNull();
            return _self.containsKey((TKey?)item.Key) && _self.containsValue((TValue?)item.Value);
        }

        /// <inheritdoc/>
        public bool ContainsKey(TKey? key)
        {
            ThrowOnNull();
            return _self.containsKey((TKey?)key);
        }

        /// <inheritdoc/>
        public void CopyTo(KeyValuePair<TKey?, TValue?>[] array, int arrayIndex)
        {
            ThrowOnNull();

            var i = 0;
            foreach (var e in this)
                array[arrayIndex + i++] = new KeyValuePair<TKey?, TValue?>(e.Key, e.Value);
        }

        /// <inheritdoc/>
        public Enumerator GetEnumerator()
        {
            ThrowOnNull();
            return new Enumerator(_self);
        }

        /// <inheritdoc/>
        public bool Remove(TKey? key)
        {
            ThrowOnNull();
            return _self.remove(key) != null;
        }

        /// <inheritdoc/>
        public bool Remove(KeyValuePair<TKey?, TValue?> item)
        {
            ThrowOnNull();
            return _self.remove(item.Key, item.Value);
        }

        /// <inheritdoc/>
        public bool TryGetValue(TKey? key, [MaybeNullWhen(false)] out TValue? value)
        {
            ThrowOnNull();

            if (_self.containsKey(key))
            {
                value = (TValue?)_self.get(key);
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
        IEnumerator<KeyValuePair<TKey?, TValue?>> IEnumerable<KeyValuePair<TKey?, TValue?>>.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

}
