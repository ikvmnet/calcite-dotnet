using System;
using System.Collections.Generic;

namespace Apache.Calcite.Data.Impl
{

    /// <summary>
    /// Entry in a schema, such as a table or sub-schema.
    /// </summary>
    public abstract class Entry
    {

        readonly CalciteSchema _schema;
        readonly string _name;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <exception cref="ArgumentNullException"></exception>
        protected Entry(CalciteSchema schema, string name)
        {
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _name = name ?? throw new ArgumentNullException(nameof(schema));
        }

        /// <summary>
        /// Gets the object's path.
        /// </summary>
        public IReadOnlyList<string> Path => _schema.GetPath(_name);

    }

}