using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

using java.lang;
using java.util;

namespace Apache.Calcite.Extensions
{

    public class CalciteConnectionPropertiesSchemaMap : IDictionary<string, string>
    {

        readonly Properties _properties;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="properties"></param>
        public CalciteConnectionPropertiesSchemaMap(Properties properties)
        {
            _properties = properties;
        }

        /// <summary>
        /// Gets or sets the schema property with the given name.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public string this[string key]
        {
            get => _properties.getProperty($"schema.{key}");
            set => _properties.setProperty($"schema.{key}", value);
        }

        /// <summary>
        /// Gets the set of matching entries.
        /// </summary>
        IEnumerable<Map.Entry> Entries => _properties.elements()
            .AsEnumerable<Map.Entry>()
            .Where(i => ((string)i.getKey()).StartsWith("schema."));

        /// <inheritdoc />
        public int Count => Entries.Count();

        /// <inheritdoc />
        ICollection<string> IDictionary<string, string>.Keys => Entries.Select(i => ((string)i.getKey())[7..]).ToList();

        /// <inheritdoc />
        ICollection<string> IDictionary<string, string>.Values => Entries.Select(i => ((string)i.getValue())).ToList();

        /// <inheritdoc />
        bool ICollection<KeyValuePair<string, string>>.IsReadOnly => false;

        /// <inheritdoc />
        public void Add(string key, string value)
        {
            _properties.setProperty($"schema.{key}", value);
        }

        /// <inheritdoc />
        public void Clear()
        {
            foreach (var entry in Entries.ToList())
                _properties.remove((string)entry.getKey());
        }

        /// <inheritdoc />
        public bool ContainsKey(string key)
        {
            return _properties.containsKey($"schema.{key}");
        }

        /// <inheritdoc />
        public IEnumerator<KeyValuePair<string, string>> GetEnumerator()
        {
            return Entries
                .Select(i => new KeyValuePair<string, string>(((string)i.getKey())[7..], (string)i.getValue()))
                .GetEnumerator();
        }

        /// <inheritdoc />
        public bool Remove(string key)
        {
            return _properties.remove($"schema.{key}") != null;
        }

        /// <inheritdoc />
        public bool TryGetValue(string key, [MaybeNullWhen(false)] out string value)
        {
            if (ContainsKey(key))
            {
                value = _properties.getProperty($"schema.{key}");
                return true;
            }

            value = null;
            return false;
        }

        void ICollection<KeyValuePair<string, string>>.Add(KeyValuePair<string, string> item)
        {
            _properties.setProperty($"schema.{item.Key}", item.Value);
        }

        bool ICollection<KeyValuePair<string, string>>.Contains(KeyValuePair<string, string> item)
        {
            if (_properties.containsKey("schema." + item.Key))
                if (_properties.getProperty("schema." + item.Key) == item.Value)
                    return true;

            return false;
        }

        void ICollection<KeyValuePair<string, string>>.CopyTo(KeyValuePair<string, string>[] array, int arrayIndex)
        {
            var items = _properties.elements()
                .AsEnumerable<Map.Entry>()
                .Where(i => ((string)i.getKey()).StartsWith("schema."))
                .Select(i => new KeyValuePair<string, string>(((string)i.getKey())[7..], (string)i.getValue()))
                .ToList();

            for (int i = 0; i < items.Count; i++)
                array[arrayIndex + i] = items[i];
        }

        bool ICollection<KeyValuePair<string, string>>.Remove(KeyValuePair<string, string> item)
        {
            if (ContainsKey(item.Key))
                if (_properties.getProperty("schema." + item.Key) == item.Value)
                    return Remove(item.Key);

            return false;
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

}
