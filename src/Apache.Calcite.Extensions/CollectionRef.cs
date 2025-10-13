using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

using org.apache.calcite.util;

namespace Apache.Calcite.Extensions
{

    /// <summary>
    /// Wrapper around a <see cref="ICollection{TValue}"/> that retypes the items.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    /// <typeparam name="TValueInfo"></typeparam>
    public readonly struct CollectionRef<TValue, TValueRef, TValueInfo> :
        IEnumerable<TValueRef>,
        ICollection<TValueRef>,
        IReadOnlyCollection<TValueRef>,
        IRef<Collection, CollectionRef<TValue, TValueRef, TValueInfo>>
        where TValue : class
        where TValueInfo : IRefInfo<TValue?, TValueRef>
    {

        /// <inheritdoc/>
        public static CollectionRef<TValue, TValueRef, TValueInfo> Create(Collection? value) => new CollectionRef<TValue, TValueRef, TValueInfo>(value);

        readonly Collection? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="collection"></param>
        public CollectionRef(Collection? collection)
        {
            _self = collection;
        }

        /// <inheritdoc />
        public readonly Collection? Underlying => _self;

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
        public readonly int Count
        {
            get
            {
                ThrowOnNull();
                return _self.size();
            }
        }

        /// <inheritdoc />
        public readonly bool IsReadOnly
        {
            get
            {
                ThrowOnNull();
                return false;
            }
        }

        /// <inheritdoc />
        public readonly void Add(TValueRef item)
        {
            ThrowOnNull();
            _self.add(TValueInfo.ToBind(item));
        }

        /// <inheritdoc />
        public readonly void Clear()
        {
            ThrowOnNull();
            _self.clear();
        }

        /// <inheritdoc />
        public readonly bool Contains(TValueRef item)
        {
            ThrowOnNull();

            if (TValueInfo.IsNull(item))
                throw new ArgumentNullException(nameof(item));

            return _self.contains(TValueInfo.ToBind(item));
        }

        /// <inheritdoc />
        public readonly void CopyTo(TValueRef[] array, int arrayIndex)
        {
            ThrowOnNull();

            if (array == null)
                throw new ArgumentNullException(nameof(array));

            var i = _self.iterator();
            var j = 0;
            while (i.hasNext())
                array[arrayIndex + j] = TValueInfo.ToCast((TValue?)i.next());
        }

        /// <inheritdoc />
        public readonly bool Remove(TValueRef item)
        {
            ThrowOnNull();
            return _self.remove(TValueInfo.ToBind(item));
        }

        /// <inheritdoc />
        public readonly IteratorRef<TValue, TValueRef> GetEnumerator()
        {
            ThrowOnNull();
            return new IteratorRef<TValue, TValueRef, TValueInfo>(_self.iterator());
        }

        /// <inheritdoc />
        readonly IEnumerator<TValueRef> IEnumerable<TValueRef>.GetEnumerator()
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
