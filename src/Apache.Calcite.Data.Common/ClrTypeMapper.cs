using System;
using System.Collections.Generic;

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

        readonly List<IClrTypeResolver> _resolvers = [];

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
        /// <param name="other"></param>
        public ClrTypeMapper(ClrTypeMapper other)
        {
            ArgumentNullException.ThrowIfNull(other);

            lock (other._resolvers)
                _resolvers.AddRange(other._resolvers);
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

            lock (_resolvers)
            {
                Remove(resolver.GetType());
                _resolvers.Insert(0, resolver);
            }

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

            lock (_resolvers)
            {
                Remove(resolver.GetType());
                _resolvers.Add(resolver);
            }

            return this;
        }

        /// <summary>
        /// Removes the resolver of a given implementation type, if present.
        /// </summary>
        /// <param name="type"></param>
        void Remove(Type type)
        {
            for (var i = 0; i < _resolvers.Count; i++)
            {
                if (_resolvers[i].GetType() == type)
                {
                    _resolvers.RemoveAt(i);
                    return;
                }
            }
        }

        /// <summary>
        /// Discards every added resolver and restores the built-in mappings.
        /// </summary>
        public void Reset()
        {
            lock (_resolvers)
            {
                _resolvers.Clear();
                _resolvers.Add(DefaultClrTypeResolver.Instance);
            }
        }

        /// <summary>
        /// Gets the resolvers in the order they will be asked.
        /// </summary>
        public IReadOnlyList<IClrTypeResolver> Resolvers
        {
            get { lock (_resolvers) return _resolvers.ToArray(); }
        }

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
