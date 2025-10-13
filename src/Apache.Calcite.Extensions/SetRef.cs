using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.util;

namespace Apache.Calcite.Extensions
{

    public readonly struct SetRef<TValue, TValueRef> :
        ICollection<TValueRef>,
        IReadOnlyCollection<TValueRef>, 
        ISet<TValueRef>,
        IReadOnlySet<TValueRef>, 
        IRef<SetRef<TValue, TValueRef>, Set>
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {

        /// <inheritdoc/>
        public static SetRef<TValue, TValueRef> Create(Set? value) => new SetRef<TValue, TValueRef>(value);

        readonly Set? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="set"></param>
        public SetRef(Set? set)
        {
            _self = set;
        }

        /// <inheritdoc />
        public readonly Set? Underlying => _self;

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
        public readonly bool Add(TValueRef item)
        {
            ThrowOnNull();
            return _self.add((TValue?)item);
        }

        /// <inheritdoc />
        public readonly void ExceptWith(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.removeAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void IntersectWith(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.retainAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsProperSubsetOf(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsProperSupersetOf(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsSubsetOf(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsSupersetOf(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool Overlaps(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool SetEquals(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void SymmetricExceptWith(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void UnionWith(IEnumerable<TValueRef> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.addAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public readonly int Count => new CollectionRef<TValue, TValueRef>(_self).Count;

        /// <inheritdoc/>
        public readonly bool IsReadOnly => new CollectionRef<TValue, TValueRef>(_self).IsReadOnly;

        readonly void ICollection<TValueRef>.Add(TValueRef item) => new CollectionRef<TValue, TValueRef>(_self).Add(item);

        /// <inheritdoc/>
        public readonly void Clear() => new CollectionRef<TValue, TValueRef>(_self).Clear();

        /// <inheritdoc/>
        public readonly bool Contains(TValueRef item) => new CollectionRef<TValue, TValueRef>(_self).Contains(item);

        /// <inheritdoc/>
        public readonly void CopyTo(TValueRef[] array, int arrayIndex) => new CollectionRef<TValue, TValueRef>(_self).CopyTo(array, arrayIndex);

        /// <inheritdoc/>
        public readonly bool Remove(TValueRef item) => new CollectionRef<TValue, TValueRef>(_self).Remove(item);

        /// <inheritdoc/>
        public readonly IEnumerator<TValueRef> GetEnumerator() => new CollectionRef<TValue, TValueRef>(_self).GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }

    public readonly struct SetRef<TValue> :
        ICollection<TValue?>, 
        IReadOnlyCollection<TValue?>, 
        ISet<TValue?>,
        IReadOnlySet<TValue?>,
        IRef<Set, SetRef<TValue>>
        where TValue : class
    {

        /// <inheritdoc/>
        public static SetRef<TValue> Create(Set? value) => new SetRef<TValue>(value);

        readonly Set? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="set"></param>
        public SetRef(Set? set)
        {
            _self = set;
        }

        /// <inheritdoc />
        public readonly Set? Underlying => _self;

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
        public readonly bool Add(TValue item)
        {
            ThrowOnNull();
            return _self.add((TValue?)item);
        }

        /// <inheritdoc />
        public readonly void ExceptWith(IEnumerable<TValue?> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.removeAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void IntersectWith(IEnumerable<TValue?> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.retainAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsProperSubsetOf(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsProperSupersetOf(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsSubsetOf(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool IsSupersetOf(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool Overlaps(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly bool SetEquals(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void SymmetricExceptWith(IEnumerable<TValue?> other)
        {
            ThrowOnNull();
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public readonly void UnionWith(IEnumerable<TValue?> other)
        {
            ThrowOnNull();

            if (other is IRef<Collection> c)
                _self.addAll(c.Underlying);
            else
                throw new NotImplementedException();
        }

        /// <inheritdoc/>
        public readonly int Count => new CollectionRef<TValue>(_self).Count;

        /// <inheritdoc/>
        public readonly bool IsReadOnly => new CollectionRef<TValue>(_self).IsReadOnly;

        readonly void ICollection<TValue?>.Add(TValue? item) => new CollectionRef<TValue>(_self).Add(item);

        /// <inheritdoc/>
        public readonly void Clear() => new CollectionRef<TValue>(_self).Clear();

        /// <inheritdoc/>
        public readonly bool Contains(TValue? item) => new CollectionRef<TValue>(_self).Contains(item);

        /// <inheritdoc/>
        public readonly void CopyTo(TValue?[] array, int arrayIndex) => new CollectionRef<TValue>(_self).CopyTo(array, arrayIndex);

        /// <inheritdoc/>
        public readonly bool Remove(TValue? item) => new CollectionRef<TValue>(_self).Remove(item);

        /// <inheritdoc/>
        public readonly IEnumerator<TValue> GetEnumerator() => new CollectionRef<TValue>(_self).GetEnumerator();

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    }

}
