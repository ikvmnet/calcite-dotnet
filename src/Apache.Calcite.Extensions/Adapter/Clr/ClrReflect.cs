using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// The members of a CLR type that make up a row, in the order they make it up.
    /// </summary>
    /// <remarks>
    /// The one answer to "what are this type's columns", asked by the row type and by the field access alike,
    /// so that an ordinal cannot come to mean two things. <c>ReflectiveSchema</c> has no counterpart to this
    /// because Java's answer is <c>Class.getFields()</c> and nothing else: the members it exposes are fields,
    /// they are already ordered, and there is nothing to decide.
    ///
    /// <para><b>.NET has properties, and IKVM does not show one to Java at all.</b> Measured: a class whose
    /// members are properties answers nothing from <c>getFields()</c> — a property is a <c>get_</c>/<c>set_</c>
    /// method pair to Java and its backing field is private under a mangled name. So
    /// <c>JavaTypeFactoryImpl.createType</c> over such a class returns <c>RecordType()</c>, a row of no
    /// columns, and does not complain. Everything here exists because that walk cannot be reused.</para>
    /// </remarks>
    public static class ClrReflect
    {

        /// <summary>
        /// One member of a row, before it is presented to Calcite as anything.
        /// </summary>
        /// <param name="Member">The property or field itself, which is what an access compiles to.</param>
        /// <param name="Type">Its type.</param>
        /// <param name="Nullable">Whether a value of it can be null.</param>
        public readonly record struct Column(MemberInfo Member, Type Type, bool Nullable);

        static readonly ConcurrentDictionary<Type, IReadOnlyList<Column>> Cache = new();

        /// <summary>
        /// Returns the members of <paramref name="type"/> that are columns of a row of it.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static IReadOnlyList<Column> Members(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Cache.GetOrAdd(type, Discover);
        }

        const BindingFlags Instance = BindingFlags.Public | BindingFlags.Instance;

        /// <summary>
        /// Works out the members of a type and their order.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// Public instance properties that can be read, and public instance fields — the fields because a type
        /// written the way Java writes one keeps working, and it is what <c>ReflectiveSchema</c> would have
        /// taken. An indexer is not a column: it is not a value the row has, and there is no name to give it.
        /// A write-only property has nothing to read.
        /// </remarks>
        static IReadOnlyList<Column> Discover(Type type)
        {
            var members = new List<Column>();

            foreach (var property in type.GetProperties(Instance))
            {
                if (property.GetMethod is not { IsPublic: true })
                    continue;
                if (property.GetIndexParameters().Length > 0)
                    continue;

                members.Add(new Column(property, property.PropertyType, Nullable(property.PropertyType)));
            }

            foreach (var field in type.GetFields(Instance))
                members.Add(new Column(field, field.FieldType, Nullable(field.FieldType)));

            // .NET promises no order from GetProperties or GetFields, and SELECT * is positional. The token is
            // declaration order for a type declared in one place, which is every type this is meant for, and it
            // is at least stable for one that is not.
            members.Sort(static (x, y) => x.Member.MetadataToken.CompareTo(y.Member.MetadataToken));

            return Reorder(type, members);
        }

        /// <summary>
        /// Returns whether a value of a type can be null.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// A reference is nullable and a value type is not, which is the answer Java gives and the answer the
        /// runtime enforces. <b>The nullable reference annotations are deliberately not read.</b> They would
        /// give a better row type — <c>string</c> and <c>string?</c> differ, where Java has to call every
        /// reference nullable — but nothing enforces them, and a column declared NOT NULL is a promise Calcite
        /// generates code against. Reading them makes the row type a claim about the source rather than a fact
        /// about it, and the failure is a null arriving where a plan was built on there being none.
        /// </remarks>
        static bool Nullable(Type type)
        {
            return type.IsValueType == false || System.Nullable.GetUnderlyingType(type) != null;
        }

        /// <summary>
        /// Returns the constructor that takes every member of <paramref name="type"/>, or
        /// <see langword="null"/> where there is none.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// What <c>JavaRowFormat.CUSTOM.record</c> will call to build a row of this type back, so its absence
        /// is what decides that a table over the type has to project rather than keep its objects.
        /// </remarks>
        public static System.Reflection.ConstructorInfo? Constructor(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            var members = Members(type);
            foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
                if (Matches(constructor, members))
                    return constructor;

            return null;
        }

        /// <summary>
        /// Returns whether a constructor takes exactly the given members, by name and type.
        /// </summary>
        static bool Matches(ConstructorInfo constructor, IReadOnlyList<Column> members)
        {
            var parameters = constructor.GetParameters();
            if (parameters.Length != members.Count || parameters.Length == 0)
                return false;

            for (int i = 0; i < parameters.Length; i++)
                if (string.Equals(parameters[i].Name, members[i].Member.Name, StringComparison.OrdinalIgnoreCase) == false
                    || parameters[i].ParameterType != members[i].Type)
                    return false;

            return true;
        }

        /// <summary>
        /// Puts the members in the order a constructor takes them, where one takes them all.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="members"></param>
        /// <returns></returns>
        /// <remarks>
        /// A row of this type is built by calling a constructor of exactly these members — Calcite's
        /// <c>JavaRowFormat.CUSTOM.record</c> emits <c>new_(rowClass, expressions)</c> in field order — so
        /// where such a constructor exists its parameter order is the authority on which column is which. That
        /// is a positional record's primary constructor, and a type written with one.
        ///
        /// <para>Where there is none the token order stands, and a query that projects every column will fail
        /// to find a constructor rather than build a row with the values transposed.</para>
        /// </remarks>
        static IReadOnlyList<Column> Reorder(Type type, List<Column> members)
        {
            foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
            {
                var parameters = constructor.GetParameters();
                if (parameters.Length != members.Count || parameters.Length == 0)
                    continue;

                var ordered = new List<Column>(members.Count);
                foreach (var parameter in parameters)
                {
                    var index = members.FindIndex(m => string.Equals(m.Member.Name, parameter.Name, StringComparison.OrdinalIgnoreCase));
                    if (index < 0 || members[index].Type != parameter.ParameterType)
                    {
                        ordered.Clear();
                        break;
                    }

                    ordered.Add(members[index]);
                }

                if (ordered.Count == members.Count)
                    return ordered;
            }

            return members;
        }

    }

}
