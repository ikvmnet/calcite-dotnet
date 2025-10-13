using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

using org.apache.calcite.util;

namespace Apache.Calcite.Extensions
{

    public readonly struct ListRef<TValue, TValueRef, TValueConv> :
        ICollection<TValueRef>,
        IReadOnlyCollection<TValueRef>,
        IList<TValueRef>,
        IReadOnlyList<TValueRef>,
        IRef<List, ListRef<TValue, TValueRef, TValueConv>>
        where TValue : class
        where TValueConv : IRefInfo<TValue?, TValueRef>
    {

        /// <inheritdoc/>
        public static ListRef<TValue, TValueRef, TValueConv> Create(List? value) => new ListRef<TValue, TValueRef, TValueConv>(value);

        readonly List? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="self"></param>
        public ListRef(List? self)
        {
            _self = self;
        }

        /// <inheritdoc />
        [MemberNotNullWhen(false, nameof(_self))]
        public readonly bool IsNull => _self == null;

        /// <inheritdoc />
        public readonly List? Underlying => _self;

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
        public TValueRef this[int index]
        {
            get
            {
                ThrowOnNull();
                return TValueConv.ToCast((TValue?)_self.get(index));
            }
            set
            {
                ThrowOnNull();
                _self.set(index, TValueConv.ToBind(value));
            }
        }

        /// <inheritdoc />
        public int IndexOf(TValueRef item)
        {
            ThrowOnNull();
            return _self.indexOf(item);
        }

        /// <inheritdoc />
        public void Insert(int index, TValueRef item)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public void RemoveAt(int index)
        {
            ThrowOnNull();
            _self.remove(index);
        }


        /// <inheritdoc />
        public int Count => new CollectionRef<TValue, TValueRef, TValueConv>(_self).Count;

        /// <inheritdoc />
        public bool IsReadOnly => new CollectionRef<TValue, TValueRef, TValueConv>(_self).IsReadOnly;

        /// <inheritdoc />
        public void Add(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueConv>(_self).Add(item);

        /// <inheritdoc />
        public void Clear() => new CollectionRef<TValue, TValueRef, TValueConv>(_self).Clear();

        /// <inheritdoc />
        public bool Contains(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueConv>(_self).Contains(item);

        /// <inheritdoc />
        public void CopyTo(TValueRef[] array, int arrayIndex) => new CollectionRef<TValue, TValueRef, TValueConv>(_self).CopyTo(array, arrayIndex);

        /// <inheritdoc />
        public bool Remove(TValueRef item) => new CollectionRef<TValue, TValueRef, TValueConv>(_self).Remove(item);

        /// <inheritdoc />
        public IteratorRef<TValue, TValueRef> GetEnumerator() => new CollectionRef<TValue, TValueRef, TValueConv>(_self).GetEnumerator();

        /// <inheritdoc />
        IEnumerator<TValueRef> IEnumerable<TValueRef>.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

    public readonly struct ListRef<TValue> :
        ICollection<TValue?>,
        IReadOnlyCollection<TValue?>,
        IList<TValue?>,
        IReadOnlyList<TValue?>,
        IRef<List, ListRef<TValue>>
        where TValue : class
    {

        /// <inheritdoc/>
        public static ListRef<TValue> Create(List? value) => new ListRef<TValue>(value);

        readonly List? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="self"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public ListRef(List? self)
        {
            _self = self;
        }

        /// <inheritdoc />
        public readonly List? Underlying => _self;

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
        public readonly TValue? this[int index]
        {
            get
            {
                ThrowOnNull();
                return (TValue?)_self.get(index);
            }
            set
            {
                ThrowOnNull();
                _self.set(index, value);
            }
        }

        /// <inheritdoc />
        public readonly int IndexOf(TValue? item)
        {
            ThrowOnNull();
            return _self.indexOf(item);
        }

        /// <inheritdoc />
        public readonly void Insert(int index, TValue? item)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void RemoveAt(int index)
        {
            ThrowOnNull();
            _self.remove(index);
        }

        /// <inheritdoc />
        public readonly int Count => new CollectionRef<TValue>(_self).Count;

        /// <inheritdoc />
        public readonly bool IsReadOnly => new CollectionRef<TValue>(_self).IsReadOnly;

        /// <inheritdoc />
        public readonly void Add(TValue? item) => new CollectionRef<TValue>(_self).Add(item);

        /// <inheritdoc />
        public readonly void Clear() => new CollectionRef<TValue>(_self).Clear();

        /// <inheritdoc />
        public readonly bool Contains(TValue? item) => new CollectionRef<TValue>(_self).Contains(item);

        /// <inheritdoc />
        public readonly void CopyTo(TValue?[] array, int arrayIndex) => new CollectionRef<TValue>(_self).CopyTo(array, arrayIndex);

        /// <inheritdoc />
        public readonly bool Remove(TValue? item) => new CollectionRef<TValue>(_self).Remove(item);

        /// <inheritdoc />
        public readonly IteratorRef<TValue> GetEnumerator() => new CollectionRef<TValue>(_self).GetEnumerator();

        /// <inheritdoc />
        readonly IEnumerator<TValue> IEnumerable<TValue?>.GetEnumerator()
        {
            return GetEnumerator();
        }

        /// <inheritdoc />
        readonly IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

}
