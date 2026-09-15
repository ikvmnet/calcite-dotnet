using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The chain of resolvers a connection or a data source will answer type questions with, and where a
    /// caller adds its own.
    /// </summary>
    /// <remarks>
    /// Configuration only: it holds no type factory and answers no lookups. A session binds it to the type
    /// factory it created and gets a <see cref="ClrTypeRegistry"/>, because what a Calcite type is held in
    /// is the type factory's answer and two sessions need not agree.
    /// </remarks>
    public sealed class ClrTypeMapper
    {

        /// <summary>
        /// The chain, replaced whole rather than mutated.
        /// </summary>
        /// <remarks>
        /// <b>Immutable so that reading it costs nothing.</b> Every connection reads this chain when it
        /// opens and almost none of them change it, so the cost that matters is the read and the copy, not
        /// the change. A list behind a lock made both allocate — the copy constructor took the lock and
        /// copied the elements, and <see cref="Resolvers"/> allocated an array per call — where replacing
        /// an immutable one makes a read a field load and a copy a reference assignment. A change allocates
        /// instead, which is the right way round for something configured once and read per connection.
        /// </remarks>
        ImmutableArray<IClrTypeResolver> _resolvers;

        /// <summary>
        /// Initializes a new instance carrying the built-in mappings.
        /// </summary>
        public ClrTypeMapper()
        {
            Reset();
        }

        /// <summary>
        /// Initializes a new instance carrying the same resolvers as another.
        /// </summary>
        /// <param name="other">The mapper to copy the chain of.</param>
        /// <remarks>
        /// One reference, because the chain is immutable: what this copies is which chain, and a change to
        /// either mapper afterwards replaces its own reference and leaves the other's alone.
        /// </remarks>
        public ClrTypeMapper(ClrTypeMapper other)
        {
            ArgumentNullException.ThrowIfNull(other);

            _resolvers = other._resolvers;
        }

        /// <summary>
        /// Puts a resolver in front of every other, so that it answers first.
        /// </summary>
        /// <param name="resolver"></param>
        /// <returns></returns>
        /// <remarks>
        /// This is the usual direction. A resolver added at the front overrides the built-in answer for the
        /// types it claims and passes everything else along by answering <see langword="null"/>. Adding a
        /// resolver of a type already present moves it rather than duplicating it, so registering twice is
        /// the same as registering once.
        /// </remarks>
        public ClrTypeMapper Prepend(IClrTypeResolver resolver)
        {
            ArgumentNullException.ThrowIfNull(resolver);

            Replace(resolver, static (chain, r) => chain.Insert(0, r));
            return this;
        }

        /// <summary>
        /// Puts a resolver behind every other, so that it answers only what nothing else claimed.
        /// </summary>
        /// <param name="resolver"></param>
        /// <returns></returns>
        public ClrTypeMapper Append(IClrTypeResolver resolver)
        {
            ArgumentNullException.ThrowIfNull(resolver);

            Replace(resolver, static (chain, r) => chain.Add(r));
            return this;
        }

        /// <summary>
        /// Replaces the chain with one that has the resolver put where <paramref name="place"/> puts it.
        /// </summary>
        /// <param name="resolver">The resolver to add.</param>
        /// <param name="place">Where in the chain it goes.</param>
        /// <remarks>
        /// A resolver of a type already present is moved rather than duplicated, so registering twice is the
        /// same as registering once. The compare-and-swap is what makes two callers configuring one mapper
        /// safe without a lock on the read path, which is the path that matters.
        /// </remarks>
        void Replace(IClrTypeResolver resolver, Func<ImmutableArray<IClrTypeResolver>, IClrTypeResolver, ImmutableArray<IClrTypeResolver>> place)
        {
            while (true)
            {
                var current = _resolvers;

                var without = current;
                for (var i = 0; i < current.Length; i++)
                {
                    if (current[i].GetType() == resolver.GetType())
                    {
                        without = current.RemoveAt(i);
                        break;
                    }
                }

                if (ImmutableInterlocked.InterlockedCompareExchange(ref _resolvers, place(without, resolver), current) == current)
                    return;
            }
        }

        /// <summary>
        /// Discards every added resolver and restores the built-in mappings.
        /// </summary>
        public void Reset()
        {
            _resolvers = [DefaultClrTypeResolver.Instance];
        }

        /// <summary>
        /// Gets the resolvers in the order they will be asked.
        /// </summary>
        public IReadOnlyList<IClrTypeResolver> Resolvers => _resolvers;

        /// <summary>
        /// Binds these resolvers to a type factory.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        public ClrTypeRegistry Bind(JavaTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return new ClrTypeRegistry(typeFactory, Resolvers);
        }

    }

}
