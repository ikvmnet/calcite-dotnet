using System;
using System.Collections;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Types
{

    /// <summary>
    /// A mapping for a Calcite type built out of other Calcite types — an <c>ARRAY</c>, a
    /// <c>MULTISET</c>, a <c>MAP</c>, a <c>ROW</c> — which carries each part across through the mapping
    /// that part resolves to.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The reason it composes rather than calling <see cref="CalciteValues"/> for the whole value is that
    /// the parts are separately claimable. A resolver that reads a <c>VARCHAR</c> as a <see cref="Uri"/>
    /// has said what a <c>VARCHAR ARRAY</c> holds, and the array of it follows; a mapping that converted
    /// the collection itself would answer <see cref="string"/><c>[]</c> while
    /// <see cref="ClrTypeRegistry.GetClrType"/> said <see cref="Uri"/><c>[]</c>, and a reader that
    /// disagrees with its own metadata is worse than one that does neither. This is what
    /// <see cref="ClrTypeContext.Registry"/> exists for.
    /// </para>
    /// <para>
    /// The element type of what comes back is still measured from the converted values, as
    /// <see cref="CalciteValues"/> measures it, so a component holding a null materializes as
    /// <c>Nullable{T}[]</c> — a fact about the values rather than about the type, which is why the type
    /// the column advertises is the non-null one.
    /// </para>
    /// </remarks>
    public sealed class CompositeClrTypeMapping : ClrTypeMapping
    {

        readonly ClrTypeRegistry _registry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context"></param>
        /// <param name="relType"></param>
        /// <param name="clrType"></param>
        public CompositeClrTypeMapping(ClrTypeContext context, RelDataType relType, Type clrType) :
            base(context, relType, clrType)
        {
            ArgumentNullException.ThrowIfNull(context);

            _registry = context.Registry;
        }

        /// <inheritdoc />
        public override object? FromCalcite(object value)
        {
            if (RelType.isStruct() && value is object[] fields)
            {
                var list = RelType.getFieldList();
                var row = new object?[fields.Length];
                for (var i = 0; i < fields.Length; i++)
                    row[i] = i < list.size() ? _registry.FromCalcite(null, ((RelDataTypeField)list.get(i)).getType(), fields[i]) : fields[i];

                return row;
            }

            switch (RelType.getSqlTypeName().name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    {
                        if (value is java.util.Collection source && RelType.getComponentType() is RelDataType component)
                        {
                            var items = new object?[source.size()];
                            var n = 0;
                            for (var i = source.iterator(); i.hasNext();)
                                items[n++] = _registry.FromCalcite(null, component, i.next());

                            return CalciteValues.Pack(items);
                        }

                        break;
                    }

                case nameof(SqlTypeName.MAP):
                    {
                        if (value is java.util.Map map && RelType.getKeyType() is RelDataType keyType && RelType.getValueType() is RelDataType valueType)
                        {
                            var count = map.size();
                            var keys = new object?[count];
                            var values = new object?[count];

                            var n = 0;
                            for (var i = map.entrySet().iterator(); i.hasNext();)
                            {
                                var entry = (java.util.Map.Entry)i.next();
                                keys[n] = _registry.FromCalcite(null, keyType, entry.getKey());
                                values[n] = _registry.FromCalcite(null, valueType, entry.getValue());
                                n++;
                            }

                            return CalciteValues.PackMap(keys, values);
                        }

                        break;
                    }
            }

            // a value that is not the shape the type says it is, which is what a schema of this runtime
            // supplying its own rows produces; the runtime class is all there is to go on
            return CalciteValues.ToClr(value, RelType);
        }

        /// <inheritdoc />
        public override object? ToCalcite(object value)
        {
            switch (RelType.getSqlTypeName().name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    {
                        // a string enumerates and is not a collection
                        if (value is IEnumerable sequence and not string && RelType.getComponentType() is RelDataType component)
                        {
                            var list = new java.util.ArrayList();
                            foreach (var item in sequence)
                                list.add(_registry.ToCalcite(null, component, item));

                            return list;
                        }

                        break;
                    }

                case nameof(SqlTypeName.MAP):
                    {
                        if (value is IDictionary dictionary && RelType.getKeyType() is RelDataType keyType && RelType.getValueType() is RelDataType valueType)
                        {
                            // a LinkedHashMap because Calcite's own SqlFunctions.map builds one: the
                            // entries of a map come out in the order they went in
                            var map = new java.util.LinkedHashMap();
                            for (var i = dictionary.GetEnumerator(); i.MoveNext();)
                                map.put(_registry.ToCalcite(null, keyType, i.Key), _registry.ToCalcite(null, valueType, i.Value));

                            return map;
                        }

                        break;
                    }
            }

            return CalciteValues.ToJava(value, RelType);
        }

    }

}
