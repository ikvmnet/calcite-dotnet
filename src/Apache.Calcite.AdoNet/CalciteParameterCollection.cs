using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.AdoNet
{

    /// <summary>
    /// Represents a collection of parameters associated with a <see cref="CalciteCommand"/>.
    /// </summary>
    public sealed class CalciteParameterCollection : DbParameterCollection
    {

        readonly List<CalciteParameter> _items = new();
        readonly object _syncRoot = new();

        /// <inheritdoc />
        public override int Count => _items.Count;

        /// <inheritdoc />
        public override object SyncRoot => _syncRoot;

        /// <inheritdoc />
        public override int Add(object value)
        {
            var p = AsParameter(value);
            _items.Add(p);
            return _items.Count - 1;
        }

        /// <summary>
        /// Adds a strongly typed parameter to the collection.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public CalciteParameter Add(CalciteParameter value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            _items.Add(value);
            return value;
        }

        /// <inheritdoc />
        public override void AddRange(Array values)
        {
            if (values is null)
                throw new ArgumentNullException(nameof(values));

            foreach (var v in values)
                Add(v!);
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
        public override int IndexOf(string parameterName)
        {
            for (var i = 0; i < _items.Count; i++)
                if (string.Equals(_items[i].ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                    return i;

            return -1;
        }

        /// <inheritdoc />
        public override void Insert(int index, object value)
        {
            _items.Insert(index, AsParameter(value));
        }

        /// <inheritdoc />
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
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var i = IndexOf(parameterName);
            if (i < 0)
                _items.Add((CalciteParameter)value);
            else
                _items[i] = (CalciteParameter)value;
        }

        /// <summary>
        /// Returns the underlying list of parameters.
        /// </summary>
        internal IReadOnlyList<CalciteParameter> Items => _items;

        static CalciteParameter AsParameter(object value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            if (value is CalciteParameter p)
                return p;

            throw new InvalidCastException($"Parameters of type '{value.GetType()}' are not supported by '{nameof(CalciteParameterCollection)}'.");
        }

    }

}
