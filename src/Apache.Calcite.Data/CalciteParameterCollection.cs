using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents the parameters of a <see cref="CalciteCommand"/> or <see cref="CalciteBatchCommand"/>. This
    /// class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A parameter's position in the collection decides which positional <c>?</c> placeholder in the command
    /// text it binds to: the first parameter binds to the first placeholder, and so on.
    /// </para>
    /// <para>
    /// Only <see cref="CalciteParameter"/> instances can be added. Lookups by name compare
    /// <see cref="DbParameter.ParameterName"/> ignoring case and use the first match.
    /// </para>
    /// </remarks>
    public sealed class CalciteParameterCollection : DbParameterCollection
    {

        readonly List<CalciteParameter> _items = new();
        readonly object _syncRoot = new();

        /// <inheritdoc />
        public override int Count => _items.Count;

        /// <inheritdoc />
        public override object SyncRoot => _syncRoot;

        /// <inheritdoc />
        /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidCastException"><paramref name="value"/> is not a <see cref="CalciteParameter"/>.</exception>
        public override int Add(object value)
        {
            var p = AsParameter(value);
            _items.Add(p);
            return _items.Count - 1;
        }

        /// <summary>
        /// Adds the specified <see cref="CalciteParameter"/> to the end of the collection.
        /// </summary>
        /// <param name="value">The parameter to add.</param>
        /// <returns>The same <paramref name="value"/> instance that was added.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
        public CalciteParameter Add(CalciteParameter value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            _items.Add(value);
            return value;
        }

        /// <inheritdoc />
        /// <exception cref="ArgumentNullException"><paramref name="values"/> or one of its elements is
        /// <see langword="null"/>.</exception>
        /// <exception cref="InvalidCastException">An element is not a <see cref="CalciteParameter"/>. Elements before
        /// it have already been added.</exception>
        public override void AddRange(Array values)
        {
            if (values is null)
                throw new ArgumentNullException(nameof(values));

            foreach (var v in values)
                _items.Add(AsParameter(v));
        }

        /// <inheritdoc />
        public override void Clear()
        {
            _items.Clear();
        }

        /// <inheritdoc />
        public override bool Contains(object value)
        {
            return value is CalciteParameter p && _items.Contains(p);
        }

        /// <inheritdoc />
        /// <remarks>
        /// The name is compared ignoring case.
        /// </remarks>
        public override bool Contains(string value)
        {
            return IndexOf(value) >= 0;
        }

        /// <inheritdoc />
        public override void CopyTo(Array array, int index)
        {
            ((ICollection)_items).CopyTo(array, index);
        }

        /// <inheritdoc />
        public override IEnumerator GetEnumerator()
        {
            return _items.GetEnumerator();
        }

        /// <inheritdoc />
        protected override DbParameter GetParameter(int index)
        {
            return _items[index];
        }

        /// <inheritdoc />
        /// <exception cref="IndexOutOfRangeException">No parameter has that name.</exception>
        protected override DbParameter GetParameter(string parameterName)
        {
            var i = IndexOf(parameterName);
            if (i < 0)
                throw new IndexOutOfRangeException($"Parameter '{parameterName}' was not found.");

            return _items[i];
        }

        /// <inheritdoc />
        public override int IndexOf(object value)
        {
            return value is CalciteParameter p ? _items.IndexOf(p) : -1;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The name is compared ignoring case, and the first match is returned.
        /// </remarks>
        public override int IndexOf(string parameterName)
        {
            for (var i = 0; i < _items.Count; i++)
                if (string.Equals(_items[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                    return i;

            return -1;
        }

        /// <inheritdoc />
        /// <exception cref="ArgumentNullException"><paramref name="value"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidCastException"><paramref name="value"/> is not a <see cref="CalciteParameter"/>.</exception>
        public override void Insert(int index, object value)
        {
            _items.Insert(index, AsParameter(value));
        }

        /// <inheritdoc />
        /// <remarks>
        /// Does nothing where <paramref name="value"/> is not in the collection.
        /// </remarks>
        public override void Remove(object value)
        {
            if (value is CalciteParameter p)
                _items.Remove(p);
        }

        /// <inheritdoc />
        public override void RemoveAt(int index)
        {
            _items.RemoveAt(index);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Does nothing where no parameter has that name.
        /// </remarks>
        public override void RemoveAt(string parameterName)
        {
            var i = IndexOf(parameterName);
            if (i >= 0)
                _items.RemoveAt(i);
        }

        /// <inheritdoc />
        protected override void SetParameter(int index, DbParameter value)
        {
            _items[index] = (CalciteParameter)value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Where no parameter has that name, <paramref name="value"/> is added to the end of the collection.
        /// </remarks>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var i = IndexOf(parameterName);
            if (i < 0)
                _items.Add((CalciteParameter)value);
            else
                _items[i] = (CalciteParameter)value;
        }

        /// <summary>
        /// Gets the parameters in binding order.
        /// </summary>
        internal IReadOnlyList<CalciteParameter> Items => _items;

        /// <summary>
        /// Returns <paramref name="value"/> as a <see cref="CalciteParameter"/>, refusing null and any other type.
        /// </summary>
        static CalciteParameter AsParameter(object? value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            if (value is CalciteParameter p)
                return p;

            throw new InvalidCastException($"Parameters of type '{value.GetType()}' are not supported by '{nameof(CalciteParameterCollection)}'.");
        }

    }

}
