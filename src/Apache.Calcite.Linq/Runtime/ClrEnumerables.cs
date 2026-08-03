using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// The sequence operators a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built
    /// from.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>EnumerableDefaults</c>, taking the same arguments under .NET names. Most
    /// of these are what <see cref="Enumerable"/> already does and say so; the ones that are not are the ones
    /// SQL needs and .NET has no operator for.
    /// </remarks>
    public static class ClrEnumerables
    {

        /// <summary>
        /// Returns the first field of each row.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A one column result is the value, not a one element row. Calcite ends a plan the same way, with
        /// <c>Enumerables.slice0</c>.
        /// </remarks>
        public static IEnumerable<object> Slice0(IEnumerable<object[]> source)
        {
            ArgumentNullException.ThrowIfNull(source);

            foreach (var row in source)
                yield return row[0];
        }

        /// <summary>
        /// Filters and projects in one pass.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="predicate">Condition each row must satisfy, or null to keep every row.</param>
        /// <param name="selector"></param>
        /// <returns></returns>
        /// <remarks>
        /// What Calcite's <c>EnumerableCalc</c> generates an anonymous <c>Enumerator</c> for: <c>moveNext</c>
        /// advances the input until the condition holds, and <c>current</c> projects. Generated Java source is
        /// the only place Calcite can put a custom enumerator; here it is an ordinary iterator, so the two
        /// steps are still one pass over the input.
        /// </remarks>
        public static IEnumerable<TResult> Calc<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, bool>? predicate, Func<TSource, TResult> selector)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentNullException.ThrowIfNull(selector);

            foreach (var row in source)
                if (predicate == null || predicate(row))
                    yield return selector(row);
        }

        /// <summary>
        /// Returns the rows that satisfy a condition.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="predicate"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Where<TSource>(IEnumerable<TSource> source, Func<TSource, bool> predicate)
        {
            return source.Where(predicate);
        }

        /// <summary>
        /// Projects each row into a new form.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        /// <param name="source"></param>
        /// <param name="selector"></param>
        /// <returns></returns>
        public static IEnumerable<TResult> Select<TSource, TResult>(IEnumerable<TSource> source, Func<TSource, TResult> selector)
        {
            return source.Select(selector);
        }

        /// <summary>
        /// Orders rows by a key.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <param name="comparator">Comparison of two keys, or null to compare them naturally.</param>
        /// <returns></returns>
        /// <remarks>
        /// The comparator is Java's, because that is what <c>PhysType.generateCollationKey</c> yields and by
        /// two different routes: a method call returning one when there is a single collation, and an
        /// anonymous class when there are several. Taking the interface is what lets both arrive here.
        /// </remarks>
        public static IEnumerable<TSource> OrderBy<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector, java.util.Comparator? comparator)
        {
            return comparator == null
                ? source.OrderBy(keySelector)
                : source.OrderBy(keySelector, Comparer<TKey>.Create((x, y) => comparator.compare(x, y)));
        }

        /// <summary>
        /// Skips a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Skip<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Skip(count);
        }

        /// <summary>
        /// Takes a number of rows.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Take<TSource>(IEnumerable<TSource> source, int count)
        {
            return source.Take(count);
        }

        /// <summary>
        /// Returns the rows of an array.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// What a VALUES clause becomes, which Calcite spells <c>Linq4j.asEnumerable</c>.
        /// </remarks>
        public static IEnumerable<TSource> AsEnumerable<TSource>(TSource[] source)
        {
            return source;
        }

        /// <summary>
        /// Returns a sequence of one row.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <param name="element"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> Singleton<TSource>(TSource element)
        {
            yield return element;
        }

        /// <summary>
        /// Returns an empty sequence.
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <returns></returns>
        public static IEnumerable<TSource> Empty<TSource>()
        {
            return [];
        }

    }

}
