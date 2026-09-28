using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Threading;

using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Builds the chain of resolvers that maps Calcite types to CLR types, and is where a caller adds its own
    /// resolvers.
    /// </summary>
    /// <remarks>
    /// A mapper starts with <see cref="DefaultClrTypeResolver"/> and answers no lookups itself. Its chain,
    /// <see cref="Resolvers"/>, is an immutable snapshot; <see cref="Bind"/> combines it with a session's type
    /// factory into a <see cref="ClrTypeRegistry"/>, which does the lookups. Changes made to a mapper after a
    /// chain has been taken from it do not affect that chain. The members are safe to call from multiple
    /// threads.
    /// </remarks>
    public sealed class ClrTypeMapper
    {

        /// <summary>
        /// The chain, replaced whole on every change so that reading or copying it needs no lock.
        /// </summary>
        ImmutableArray<IClrTypeResolver> _resolvers;

        /// <summary>
        /// Initializes a new instance whose chain holds only the built-in mappings.
        /// </summary>
        public ClrTypeMapper()
        {
            Reset();
        }

        /// <summary>
        /// Initializes a new instance with the same chain as another.
        /// </summary>
        /// <param name="other">The mapper to copy the chain of.</param>
        /// <remarks>
        /// Later changes to either mapper do not affect the other.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="other"/> is <see langword="null"/>.</exception>
        public ClrTypeMapper(ClrTypeMapper other)
        {
            ArgumentNullException.ThrowIfNull(other);

            _resolvers = other._resolvers;
        }

        /// <summary>
        /// Initializes a new instance with a given chain.
        /// </summary>
        /// <param name="resolvers">The chain, in the order it is asked. The sequence is copied.</param>
        /// <exception cref="ArgumentNullException"><paramref name="resolvers"/> is <see langword="null"/>.</exception>
        public ClrTypeMapper(IEnumerable<IClrTypeResolver> resolvers)
        {
            ArgumentNullException.ThrowIfNull(resolvers);

            _resolvers = [.. resolvers];
        }

        /// <summary>
        /// Puts a resolver at the front of the chain, so that it is asked first.
        /// </summary>
        /// <param name="resolver">The resolver.</param>
        /// <returns>This mapper.</returns>
        /// <remarks>
        /// A resolver at the front overrides the built-in mappings for the types it answers, and passes every
        /// other lookup on by returning <see langword="null"/>. A resolver of the same runtime type already
        /// in the chain is removed first, so adding one twice leaves a single instance.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="resolver"/> is <see langword="null"/>.</exception>
        public ClrTypeMapper Prepend(IClrTypeResolver resolver)
        {
            ArgumentNullException.ThrowIfNull(resolver);

            Replace(resolver, static (chain, r) => chain.Insert(0, r));
            return this;
        }

        /// <summary>
        /// Puts a resolver at the end of the chain, so that it answers only lookups no other resolver
        /// answers.
        /// </summary>
        /// <param name="resolver">The resolver.</param>
        /// <returns>This mapper.</returns>
        /// <remarks>
        /// A resolver of the same runtime type already in the chain is removed first.
        /// </remarks>
        /// <exception cref="ArgumentNullException"><paramref name="resolver"/> is <see langword="null"/>.</exception>
        public ClrTypeMapper Append(IClrTypeResolver resolver)
        {
            ArgumentNullException.ThrowIfNull(resolver);

            Replace(resolver, static (chain, r) => chain.Add(r));
            return this;
        }

        /// <summary>
        /// Replaces the chain with one in which any resolver of the same runtime type as
        /// <paramref name="resolver"/> is removed and <paramref name="resolver"/> is placed by
        /// <paramref name="place"/>, retrying until the compare-and-swap succeeds.
        /// </summary>
        /// <param name="resolver">The resolver to add.</param>
        /// <param name="place">Inserts the resolver into the chain.</param>
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
        /// Replaces the chain with one holding only the built-in mappings.
        /// </summary>
        public void Reset()
        {
            _resolvers = [DefaultClrTypeResolver.Instance];
        }

        /// <summary>
        /// Gets the chain, in the order its resolvers are asked.
        /// </summary>
        /// <remarks>
        /// The array is immutable, so it can be kept as a fixed chain; later changes to this mapper do not
        /// affect it.
        /// </remarks>
        public ImmutableArray<IClrTypeResolver> Resolvers => _resolvers;

        /// <summary>
        /// Returns a registry that answers lookups with the current chain against a type factory.
        /// </summary>
        /// <param name="typeFactory">The type factory of the session the registry serves.</param>
        /// <returns>The registry.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="typeFactory"/> is <see langword="null"/>.</exception>
        public ClrTypeRegistry Bind(JavaTypeFactory typeFactory)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);

            return new ClrTypeRegistry(typeFactory, Resolvers);
        }

    }

}
