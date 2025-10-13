using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

using java.lang;

namespace Apache.Calcite.Extensions
{

    /// <summary>
    /// Wrapper around a <see cref="IEnumerable{TValue}"/> that retypes the items.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    /// <typeparam name="TValueRef"></typeparam>
    public readonly struct IterableRef<TValue, TValueRef, TValueInfo> :
        IEnumerable<TValueRef>,
        IRef<Iterable, IterableRef<TValue, TValueRef, TValueInfo>>
        where TValue : class
        where TValueRef : struct, IRef<TValue, TValueRef>
    {

        /// <inheritdoc/>
        public static IterableRef<TValue, TValueRef, TValueInfo> Create(Iterable? value) => new IterableRef<TValue, TValueRef, TValueInfo>(value);

        readonly Iterable? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="self"></param>
        public IterableRef(Iterable? self)
        {
            _self = self;
        }

        /// <summary>
        /// Gets the underlying Java object.
        /// </summary>
        public readonly Iterable? Underlying => _self;

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
        public IEnumerator<TValueRef> GetEnumerator()
        {
            ThrowOnNull();
            return new IteratorRef<TValue, TValueRef>(_self.iterator());
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

    /// <summary>
    /// Wrapper around a <see cref="IEnumerable{TValue}"/> that retypes the items.
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    public readonly struct IterableRef<TValue> :
        IEnumerable<TValue?>,
        IRef<Iterable, IterableRef<TValue>>
        where TValue : class
    {

        /// <inheritdoc/>
        public static IterableRef<TValue> Create(Iterable? value) => new IterableRef<TValue>(value);

        readonly Iterable? _self;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="self"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public IterableRef(Iterable? self)
        {
            _self = self;
        }

        /// <summary>
        /// Gets the underlying Java object.
        /// </summary>
        public readonly Iterable? Underlying => _self;

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
        public IEnumerator<TValue?> GetEnumerator()
        {
            ThrowOnNull();
            return new IteratorRef<TValue>(_self.iterator());
        }

        /// <inheritdoc />
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

}
