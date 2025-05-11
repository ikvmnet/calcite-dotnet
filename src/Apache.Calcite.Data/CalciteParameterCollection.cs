using System;
using System.Collections.Generic;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Collection of parameters relevant to a <see cref="CalciteCommand"/>.
    /// </summary>
    public class CalciteParameterCollection : CalciteParameterCollectionBase, IList<CalciteParameter>
    {

        /// <summary>
        /// Gets or sets the parameter at the specified position within this collection.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        CalciteParameter IList<CalciteParameter>.this[int index]
        {
            get => (CalciteParameter)base[index];
            set => base[index] = value;
        }

        /// <summary>
        /// Adds the specified parameter to this collection.
        /// </summary>
        /// <param name="parameter"></param>
        void ICollection<CalciteParameter>.Add(CalciteParameter parameter)
        {
            Add(parameter);
        }

        /// <summary>
        /// Adds the specified parameter to this collection.
        /// </summary>
        /// <param name="parameter"></param>
        public CalciteParameter Add(CalciteParameter parameter)
        {
            base.Add(parameter);
            return parameter;
        }

        /// <summary>
        /// Adds a value to the end of the <see cref="CalciteParameterCollection"/>.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public CalciteParameter AddWithValue(string parameterName, object value)
        {
            return Add(new CalciteParameter(parameterName, value));
        }

        /// <summary>
        /// Returns <c>true</c> if this collection contains the specified parameter.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public bool Contains(CalciteParameter parameter)
        {
            if (parameter is null)
                throw new ArgumentNullException(nameof(parameter));

            return base.Contains(parameter);
        }

        /// <summary>
        /// Copies the parameters into the given array starting at the specified index.
        /// </summary>
        /// <param name="array"></param>
        /// <param name="arrayIndex"></param>
        public void CopyTo(CalciteParameter[] array, int arrayIndex)
        {
            base.CopyTo(array, arrayIndex);
        }

        /// <summary>
        /// Returns the index of the specified parameter within this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public int IndexOf(CalciteParameter parameter)
        {
            return base.IndexOf(parameter);
        }

        /// <summary>
        /// Inserts the parameter into this collection at the specified index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="parameter"></param>
        public void Insert(int index, CalciteParameter parameter)
        {
            base.Insert(index, parameter);
        }

        /// <summary>
        /// Removes the specified parameter from this collection.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public bool Remove(CalciteParameter parameter)
        {
            return base.RemoveImpl(parameter);
        }

        /// <summary>
        /// Gets an enumerator that iterates through the parameters in this collection.
        /// </summary>
        /// <returns></returns>
        public new IEnumerator<CalciteParameter> GetEnumerator()
        {
            return GetJdbcEnumerator();
        }

    }

}
