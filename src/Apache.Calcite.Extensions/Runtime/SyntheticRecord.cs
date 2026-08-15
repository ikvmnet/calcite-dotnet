using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Base of the record classes emitted for <c>JavaRowFormat.CUSTOM</c>.
    /// </summary>
    /// <remarks>
    /// Calcite generates a class per row shape with equals, hashCode, compareTo and toString written out, and
    /// hands it to Janino. An emitted type carries only its fields and its constructors; the four members are
    /// here, built once per type from those fields and against the same <see cref="Utilities"/> methods
    /// Calcite's generated bodies call.
    /// </remarks>
    public abstract class SyntheticRecord : java.lang.Comparable
    {

        /// <summary>
        /// What each record type does for the four members, built the first time one is asked for.
        /// </summary>
        static readonly ConcurrentDictionary<Type, Members> ByType = new();

        /// <summary>
        /// Returns the members of this instance's type.
        /// </summary>
        /// <returns></returns>
        Members Of() => ByType.GetOrAdd(GetType(), Members.For);

        /// <inheritdoc />
        public bool equals(object? other) => Of().IsEqual(this, other);

        /// <inheritdoc />
        public override bool Equals(object? other) => Of().IsEqual(this, other);

        /// <inheritdoc />
        public int hashCode() => Of().HashOf(this);

        /// <inheritdoc />
        public override int GetHashCode() => Of().HashOf(this);

        /// <inheritdoc />
        public override string ToString() => Of().PrintOf(this);

        /// <inheritdoc />
        public int compareTo(object? other) => Of().CompareOf(this, other);

        /// <inheritdoc />
        public int CompareTo(object? other) => Of().CompareOf(this, other);

        /// <summary>
        /// The four members of one record type.
        /// </summary>
        /// <param name="isEqual"></param>
        /// <param name="hash"></param>
        /// <param name="compare"></param>
        /// <param name="print"></param>
        sealed class Members(Func<object, object?, bool> isEqual, Func<object, int> hash, Func<object, object?, int> compare, Func<object, string> print)
        {

            /// <summary>Compares two records field by field.</summary>
            public Func<object, object?, bool> IsEqual { get; } = isEqual;

            /// <summary>Accumulates a hash over the fields.</summary>
            public Func<object, int> HashOf { get; } = hash;

            /// <summary>Orders two records field by field.</summary>
            public Func<object, object?, int> CompareOf { get; } = compare;

            /// <summary>Renders a record.</summary>
            public Func<object, string> PrintOf { get; } = print;

            /// <summary>
            /// <c>Utilities.hash(int, Object)</c> and its primitive overloads, which is what Calcite's
            /// generated hashCode accumulates with.
            /// </summary>
            static readonly MethodInfo[] Hashes = typeof(Utilities).GetMethods(BindingFlags.Public | BindingFlags.Static);

            /// <summary>
            /// Builds the members of a record type from its fields.
            /// </summary>
            /// <param name="type"></param>
            /// <returns></returns>
            public static Members For(Type type)
            {
                var fields = type.GetFields(BindingFlags.Public | BindingFlags.Instance);

                return new Members(BuildEquals(type, fields), BuildHash(type, fields), BuildCompare(type, fields), BuildPrint(type, fields));
            }

            /// <summary>
            /// Builds <c>equals</c>, which compares every field.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="fields"></param>
            /// <returns></returns>
            static Func<object, object?, bool> BuildEquals(Type type, FieldInfo[] fields)
            {
                var left = Expression.Parameter(typeof(object), "o0");
                var right = Expression.Parameter(typeof(object), "o1");
                var that = Expression.Variable(type, "that");

                Expression body = Expression.Constant(true);
                foreach (var field in fields)
                {
                    var comparison = field.FieldType.IsValueType
                        ? (Expression)Expression.Equal(Expression.Field(Expression.Convert(left, type), field), Expression.Field(that, field))
                        : Expression.Call(ObjectsEqual, Expression.Convert(Expression.Field(Expression.Convert(left, type), field), typeof(object)), Expression.Convert(Expression.Field(that, field), typeof(object)));

                    body = Expression.AndAlso(body, comparison);
                }

                var test = Expression.Condition(
                    Expression.ReferenceEqual(left, right),
                    Expression.Constant(true),
                    Expression.Condition(
                        Expression.TypeIs(right, type),
                        Expression.Block([that], Expression.Assign(that, Expression.Convert(right, type)), body),
                        Expression.Constant(false)));

                return Expression.Lambda<Func<object, object?, bool>>(test, left, right).Compile();
            }

            /// <summary>
            /// <c>Objects.equal</c>, which Calcite's generated equals uses for a reference field.
            /// </summary>
            static readonly MethodInfo ObjectsEqual = typeof(com.google.common.@base.Objects).GetMethod("equal", [typeof(object), typeof(object)])
                ?? throw new InvalidOperationException("Guava's Objects has no equal(Object, Object).");

            /// <summary>
            /// Builds <c>hashCode</c>, accumulating one field at a time.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="fields"></param>
            /// <returns></returns>
            static Func<object, int> BuildHash(Type type, FieldInfo[] fields)
            {
                var value = Expression.Parameter(typeof(object), "o");
                var self = Expression.Variable(type, "self");
                var h = Expression.Variable(typeof(int), "h");

                var body = new List<Expression>
                {
                    Expression.Assign(self, Expression.Convert(value, type)),
                    Expression.Assign(h, Expression.Constant(0)),
                };

                foreach (var field in fields)
                    body.Add(Expression.Assign(h, Expression.Call(Hash(field.FieldType), h, Expression.Field(self, field))));

                body.Add(h);

                return Expression.Lambda<Func<object, int>>(Expression.Block([self, h], body), value).Compile();
            }

            /// <summary>
            /// Returns the <c>Utilities.hash</c> overload that takes a value of the given type.
            /// </summary>
            /// <param name="type"></param>
            /// <returns></returns>
            static MethodInfo Hash(Type type)
            {
                MethodInfo? general = null;

                foreach (var candidate in Hashes)
                {
                    if (candidate.Name != "hash")
                        continue;

                    var parameters = candidate.GetParameters();
                    if (parameters.Length != 2 || parameters[0].ParameterType != typeof(int))
                        continue;

                    if (parameters[1].ParameterType == type)
                        return candidate;

                    if (parameters[1].ParameterType == typeof(object))
                        general = candidate;
                }

                return general ?? throw new NotSupportedException($"Utilities has no hash for '{type}'.");
            }

            /// <summary>
            /// Builds <c>compareTo</c>, which returns on the first field that differs.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="fields"></param>
            /// <returns></returns>
            static Func<object, object?, int> BuildCompare(Type type, FieldInfo[] fields)
            {
                var left = Expression.Parameter(typeof(object), "o0");
                var right = Expression.Parameter(typeof(object), "o1");
                var self = Expression.Variable(type, "self");
                var that = Expression.Variable(type, "that");
                var c = Expression.Variable(typeof(int), "c");
                var end = Expression.Label(typeof(int), "return");

                var body = new List<Expression>
                {
                    Expression.Assign(self, Expression.Convert(left, type)),
                    Expression.Assign(that, Expression.Convert(right, type)),
                };

                foreach (var field in fields)
                {
                    // a field the runtime cannot compare is skipped, exactly as Calcite skips one whose
                    // compare method does not exist: a record is not always used as a sorting key
                    var compare = Compare(field.FieldType);
                    if (compare == null)
                        continue;

                    body.Add(Expression.Assign(c, Expression.Call(compare, Convert(Expression.Field(self, field), compare, 0), Convert(Expression.Field(that, field), compare, 1))));
                    body.Add(Expression.IfThen(Expression.NotEqual(c, Expression.Constant(0)), Expression.Return(end, c)));
                }

                body.Add(Expression.Label(end, Expression.Constant(0)));

                return Expression.Lambda<Func<object, object?, int>>(Expression.Block([self, that, c], body), left, right).Compile();
            }

            /// <summary>
            /// Brings a field to the type the comparison takes.
            /// </summary>
            /// <param name="field"></param>
            /// <param name="method"></param>
            /// <param name="index"></param>
            /// <returns></returns>
            static Expression Convert(Expression field, MethodInfo method, int index)
            {
                var parameter = method.GetParameters()[index].ParameterType;

                return field.Type == parameter ? field : Expression.Convert(field, parameter);
            }

            /// <summary>
            /// Returns the <c>Utilities</c> method that compares a value of the given type, or null where
            /// there is none.
            /// </summary>
            /// <param name="type"></param>
            /// <returns></returns>
            /// <remarks>
            /// Calcite names the class and the method and lets <c>Types.lookupMethod</c> bind the overload
            /// from the argument types, and answers a <c>NoSuchMethodException</c> where none does — which is
            /// what leaves a field of a type nothing compares out of <c>compareTo</c>, an <see cref="object"/>
            /// being one, since it is not a <c>Comparable</c>. <see cref="ClrTypes.Resolve(Type, string,
            /// Type[])"/> is that resolution, and it throws where Calcite's throws.
            ///
            /// <para>Calcite reads <c>Types.RecordField.nullable()</c> and names <c>compareNullsLast</c> for a
            /// nullable field. That flag is not on a CLR field — <c>SyntheticRecordEmitter</c> drops it, and
            /// it cannot be recovered from the type, a String field being nullable through one of the
            /// factory's two paths and not the other — so the name here is always <c>compare</c>, and a null
            /// in a reference field throws where Calcite orders it last. Measured.</para>
            /// </remarks>
            static MethodInfo? Compare(Type type)
            {
                try
                {
                    return ClrTypes.Resolve(typeof(Utilities), "compare", [type, type]);
                }
                catch (NotSupportedException)
                {
                    return null;
                }
            }

            /// <summary>
            /// Builds <c>toString</c>.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="fields"></param>
            /// <returns></returns>
            static Func<object, string> BuildPrint(Type type, FieldInfo[] fields)
            {
                var value = Expression.Parameter(typeof(object), "o");
                var self = Expression.Variable(type, "self");

                Expression text = Expression.Constant(fields.Length == 0 ? "{}" : "{");
                for (int i = 0; i < fields.Length; i++)
                {
                    text = Expression.Call(Concat, text, Expression.Constant((i == 0 ? "" : ", ") + fields[i].Name + "="));
                    text = Expression.Call(Concat, text, Expression.Call(ValueOf, Expression.Convert(Expression.Field(self, fields[i]), typeof(object))));
                }

                if (fields.Length > 0)
                    text = Expression.Call(Concat, text, Expression.Constant("}"));

                return Expression.Lambda<Func<object, string>>(
                    Expression.Block([self], Expression.Assign(self, Expression.Convert(value, type)), text), value).Compile();
            }

            static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])
                ?? throw new InvalidOperationException("String has no Concat(string, string).");
            // Objects.toString rather than String.valueOf, whose helper IKVM keeps internal; both render a null as "null"
            static readonly MethodInfo ValueOf = typeof(java.util.Objects).GetMethod("toString", [typeof(object)])
                ?? throw new InvalidOperationException("java.util.Objects has no toString(Object).");

        }

    }

}
