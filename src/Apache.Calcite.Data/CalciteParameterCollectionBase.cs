using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// The base class for a collection of parameters relevant to a <see cref="CalciteCommand"/>.
    /// </summary>
    public abstract class CalciteParameterCollectionBase : DbParameterCollection
    {

        readonly List<CalciteParameter> _self = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        internal CalciteParameterCollectionBase()
        {

        }

        /// <summary>
        /// Gets an object that can be used to synchronize access to this object.
        /// </summary>
        public override object SyncRoot => this;

        /// <summary>
        /// Gets the number of items in the collection.
        /// </summary>
        public override int Count => _self.Count;

        /// <summary>
        /// Adds the specified object to the collection.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        public override int Add(object value)
        {
            if (value is not CalciteParameter p)
                throw new CalciteDbException("Parameter must be a JdbcParameter.");

            _self.Add(p);
            return _self.Count - 1;
        }

        /// <summary>
        /// Adds the specified array of items to this collection.
        /// </summary>
        /// <param name="values"></param>
        public override void AddRange(Array values)
        {
            foreach (var i in values)
                Add(i);
        }

        /// <summary>
        /// Clears the collection.
        /// </summary>
        public override void Clear()
        {
            _self.Clear();
        }

        /// <summary>
        /// Returns <c>true</c> if the specified item exists in the collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public override bool Contains(object parameter)
        {
            return parameter is CalciteParameter p && _self.Contains(p);
        }

        /// <summary>
        /// Returns <c>true</c> if the specified item exists in the collection.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public override bool Contains(string parameterName)
        {
            return _self.Any(i => i.ParameterName == parameterName);
        }

        /// <summary>
        /// Copies the parameters into the given array starting at the specified index.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="index"></param>
        public override void CopyTo(Array array, int index)
        {
            for (int i = 0; i < _self.Count; i++)
                array.SetValue(_self[i], index + i);
        }

        /// <summary>
        /// Returns the index of the specified parameter within this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public override int IndexOf(object parameter)
        {
            return parameter is CalciteParameter p ? _self.IndexOf(p) : -1;
        }

        /// <summary>
        /// Returns the index of the specified parameter within this collection.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        public override int IndexOf(string parameterName)
        {
            if (string.IsNullOrWhiteSpace(parameterName))
                throw new ArgumentException($"'{nameof(parameterName)}' cannot be null or whitespace.", nameof(parameterName));

            return _self.FindIndex(i => i.ParameterName == parameterName);
        }

        /// <summary>
        /// Inserts a parameter at the specified position within this collection.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="parameter"></param>
        /// <exception cref="CalciteDbException"></exception>
        public override void Insert(int index, object parameter)
        {
            if (parameter is not CalciteParameter p)
                throw new ArgumentException("Parameter must be a JdbcParameter.", nameof(parameter));

            _self.Insert(index, p);
        }

        /// <summary>
        /// Removes the specified parameter from this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected bool RemoveImpl(object parameter)
        {
            if (parameter is null)
                throw new ArgumentNullException(nameof(parameter));

            return parameter is CalciteParameter p && _self.Remove(p);
        }

        /// <summary>
        /// Removes the specified parameter from this collection.
        /// </summary>
        /// <param name="parameter"></param>
        public override void Remove(object parameter)
        {
            if (parameter is not CalciteParameter p)
                return;

            _self.Remove(p);
        }

        /// <summary>
        /// Removes the parameter at the specified index from this collection.
        /// </summary>
        /// <param name="index"></param>
        public override void RemoveAt(int index)
        {
            _self.RemoveAt(index);
        }

        /// <summary>
        /// Removes the specified parameter from this collection.
        /// </summary>
        /// <param name="parameterName"></param>
        public override void RemoveAt(string parameterName)
        {
            if (parameterName is null)
                throw new ArgumentNullException(nameof(parameterName));

            var i = IndexOf(parameterName);
            if (i > -1)
                RemoveAt(i);
        }

        /// <summary>
        /// Gets an enumerator that can be used to iterate over the contents of this collection.
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            return GetJdbcEnumerator();
        }

        /// <summary>
        /// Gets an enumerator that can be used to iterate over the contents of this collection.
        /// </summary>
        /// <returns></returns>
        protected IEnumerator<CalciteParameter> GetJdbcEnumerator()
        {
            return _self.GetEnumerator();
        }

        /// <summary>
        /// Gets the parameter at the specified index.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        protected override DbParameter GetParameter(int index)
        {
            return _self[index];
        }

        /// <summary>
        /// Gets the parameter with the specified name.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <returns></returns>
        protected override DbParameter GetParameter(string parameterName)
        {
            if (parameterName is null)
                throw new ArgumentNullException(nameof(parameterName));

            return _self.FirstOrDefault(i => i.ParameterName == parameterName) ?? throw new IndexOutOfRangeException();
        }

        /// <summary>
        /// Sets the parameter at the specified index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <exception cref="CalciteDbException"></exception>
        protected override void SetParameter(int index, DbParameter value)
        {
            if (value is not CalciteParameter p)
                throw new CalciteDbException("Parameter must be a JdbcParameter.");

            _self[index] = p;
        }

        /// <summary>
        /// Sets the parameter with the specified name.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        /// <exception cref="CalciteDbException"></exception>
        protected override void SetParameter(string parameterName, DbParameter value)
        {
            RemoveAt(parameterName);
            Add(value);
        }

    }

}
