using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// An ordered collection of <see cref="CalciteBatchCommand"/> instances that belong to a
    /// <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Commands are executed in the order they appear in this collection. Use the typed
    /// <see cref="Add(CalciteBatchCommand)"/> overload to add commands without boxing.
    /// </remarks>
    public sealed class CalciteBatchCommandCollection : DbBatchCommandCollection
    {

        readonly List<CalciteBatchCommand> _items = new();

        /// <inheritdoc />
        public override int Count => _items.Count;

        /// <inheritdoc />
        public override bool IsReadOnly => false;

        /// <inheritdoc />
        protected override DbBatchCommand GetBatchCommand(int index) => _items[index];

        /// <inheritdoc />
        protected override void SetBatchCommand(int index, DbBatchCommand batchCommand) => _items[index] = AsBatchCommand(batchCommand);

        /// <inheritdoc />
        public override void Add(DbBatchCommand item) => _items.Add(AsBatchCommand(item));

        /// <summary>
        /// Adds a <see cref="CalciteBatchCommand"/> to the end of the collection.
        /// </summary>
        /// <param name="item">The command to add.</param>
        /// <exception cref="ArgumentNullException"><paramref name="item"/> is <see langword="null"/>.</exception>
        public void Add(CalciteBatchCommand item)
        {
            if (item is null)
                throw new ArgumentNullException(nameof(item));
            _items.Add(item);
        }

        /// <inheritdoc />
        public override void Clear() => _items.Clear();

        /// <inheritdoc />
        public override bool Contains(DbBatchCommand item) => item is CalciteBatchCommand c && _items.Contains(c);

        /// <inheritdoc />
        public override void CopyTo(DbBatchCommand[] array, int arrayIndex)
        {
            if (array is null)
                throw new ArgumentNullException(nameof(array));

            for (var i = 0; i < _items.Count; i++)
                array[arrayIndex + i] = _items[i];
        }

        /// <inheritdoc />
        public override IEnumerator<DbBatchCommand> GetEnumerator()
        {
            foreach (var item in _items)
                yield return item;
        }

        /// <inheritdoc />
        public override int IndexOf(DbBatchCommand item) => item is CalciteBatchCommand c ? _items.IndexOf(c) : -1;

        /// <inheritdoc />
        public override void Insert(int index, DbBatchCommand item) => _items.Insert(index, AsBatchCommand(item));

        /// <inheritdoc />
        public override bool Remove(DbBatchCommand item)
        {
            if (item is CalciteBatchCommand c)
                return _items.Remove(c);
            return false;
        }

        /// <inheritdoc />
        public override void RemoveAt(int index) => _items.RemoveAt(index);

        internal IReadOnlyList<CalciteBatchCommand> Items => _items;

        static CalciteBatchCommand AsBatchCommand(DbBatchCommand value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));
            if (value is not CalciteBatchCommand c)
                throw new ArgumentException($"Value must be of type {nameof(CalciteBatchCommand)}.", nameof(value));
            return c;
        }

    }

}
