using System;
using System.Runtime.CompilerServices;
using System.Reflection;
using System.Reflection.Emit;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.jdbc;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Emits the CLR type behind a <see cref="JavaTypeFactoryImpl.SyntheticRecordType"/>.
    /// </summary>
    /// <remarks>
    /// For <c>JavaRowFormat.CUSTOM</c> the type factory answers the row class of a multi-field row with a
    /// synthetic record type, which Calcite only describes: Janino compiles its class from the declaration
    /// written into the generated source. An expression tree needs a real <see cref="Type"/>, so
    /// <see cref="ClassDecl"/> emits the class, with its constructors, <c>equals</c>, <c>hashCode</c>,
    /// <c>compareTo</c> and <c>toString</c>, before returning it.
    /// </remarks>
    static class SyntheticRecordEmitter
    {

        static readonly object sync = new();
        static int count;

        /// <summary>
        /// The type emitted for each record type, held only as long as the record type is alive.
        /// </summary>
        /// <remarks>
        /// A <c>SyntheticRecordType</c> is held by its <c>JavaTypeFactoryImpl</c>, which lives as long as its
        /// connection, so a weak key ties the emitted type's lifetime to the connection's. The emitted type
        /// does not refer to the record type, so the value does not keep its key alive.
        /// </remarks>
        static readonly ConditionalWeakTable<JavaTypeFactoryImpl.SyntheticRecordType, Type> emitted = new();

        /// <summary>
        /// Returns the CLR type of a synthetic record, emitting it the first time it is asked for.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>EnumerableRelImplementor.classDecl</c>, which writes a class declaration for
        /// Janino into each compilation unit. Here the class is emitted once per record type and reused.
        /// Thread-safe.
        /// </remarks>
        /// <param name="type">The synthetic record type from Calcite's type factory.</param>
        /// <returns>The emitted CLR class, the same instance for every call with the same record type.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        public static Type ClassDecl(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            ArgumentNullException.ThrowIfNull(type);

            lock (sync)
            {
                if (emitted.TryGetValue(type, out var existing))
                    return existing;

                var clr = Emit(type);
                emitted.Add(type, clr);
                return clr;
            }
        }

        /// <summary>
        /// Emits the class and its members.
        /// </summary>
        /// <remarks>
        /// Follows the body of <c>classDecl</c> section by section, emitting IL where Calcite builds a linq4j
        /// block; the members, their order, their locals and the per-field loops are Calcite's.
        /// </remarks>
        /// <param name="type">The synthetic record type.</param>
        /// <returns>The created CLR class, in a collectable assembly of its own.</returns>
        static Type Emit(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            // one collectable assembly per record type, because RunAndCollect unloads whole assemblies: a shared
            // assembly would be released only once every type in it was unreachable
            var name = $"{type.getName()}_{++count}";
            var module = AssemblyBuilder
                .DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.RunAndCollect)
                .DefineDynamicModule(name);

            // the factory names a record after its shape, so two connections can hold records of the same
            // name; the counter keeps the names distinct in stack traces
            var classDeclaration = module.DefineType(
                name,
                TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.Class,
                typeof(SyntheticRecord));

            var recordFields = RecordFields(type);
            var fields = new FieldBuilder[recordFields.Length];
            var types = new Type[recordFields.Length];

            // For each field:
            //   public T0 f0;
            //   ...
            for (int i = 0; i < recordFields.Length; i++)
            {
                types[i] = ClrTypes.Resolve(recordFields[i].getType());
                fields[i] = classDeclaration.DefineField(recordFields[i].getName(), types[i], FieldAttributes.Public);
            }

            // Constructor:
            //   Foo(T0 f0, ...) { this.f0 = f0; ... }

            // Calcite declares only a parameterless constructor, because a constructor of many parameters can
            // fail to compile, and its generated code assigns the fields
            ConstructorDecl(classDeclaration, [], fields);

            // an expression tree building a record has no statements to assign fields in, so
            // JavaRowFormat.CUSTOM.record calls this constructor instead; a record with no fields (as for a
            // semi join whose right input projects nothing) would get two identical constructors
            if (types.Length > 0)
                ConstructorDecl(classDeclaration, types, fields);

            // equals method():
            //   public boolean equals(Object o) {
            //       if (this == o) return true;
            //       if (!(o instanceof MyClass)) return false;
            //       final MyClass that = (MyClass) o;
            //       return this.f0 == that.f0
            //         && equal(this.f1, that.f1)
            //         ...
            //   }
            var blockBuilder2 = classDeclaration
                .DefineMethod(nameof(Equals), Override, typeof(bool), [typeof(object)])
                .GetILGenerator();
            var thatParameter = blockBuilder2.DeclareLocal(classDeclaration);
            var notSame = blockBuilder2.DefineLabel();
            blockBuilder2.Emit(OpCodes.Ldarg_0);
            blockBuilder2.Emit(OpCodes.Ldarg_1);
            blockBuilder2.Emit(OpCodes.Bne_Un, notSame);
            blockBuilder2.Emit(OpCodes.Ldc_I4_1);
            blockBuilder2.Emit(OpCodes.Ret);
            blockBuilder2.MarkLabel(notSame);
            var isInstance = blockBuilder2.DefineLabel();
            blockBuilder2.Emit(OpCodes.Ldarg_1);
            blockBuilder2.Emit(OpCodes.Isinst, classDeclaration);
            blockBuilder2.Emit(OpCodes.Brtrue, isInstance);
            blockBuilder2.Emit(OpCodes.Ldc_I4_0);
            blockBuilder2.Emit(OpCodes.Ret);
            blockBuilder2.MarkLabel(isInstance);
            blockBuilder2.Emit(OpCodes.Ldarg_1);
            blockBuilder2.Emit(OpCodes.Castclass, classDeclaration);
            blockBuilder2.Emit(OpCodes.Stloc, thatParameter);

            // foldAnd short-circuits: each comparison falls through to the next, and any false returns false
            var unequal = blockBuilder2.DefineLabel();
            for (int i = 0; i < recordFields.Length; i++)
            {
                blockBuilder2.Emit(OpCodes.Ldarg_0);
                blockBuilder2.Emit(OpCodes.Ldfld, fields[i]);
                blockBuilder2.Emit(OpCodes.Ldloc, thatParameter);
                blockBuilder2.Emit(OpCodes.Ldfld, fields[i]);

                if (J.Primitive.@is(recordFields[i].getType()))
                {
                    blockBuilder2.Emit(OpCodes.Ceq);
                }
                else
                {
                    blockBuilder2.Emit(OpCodes.Call, ObjectsEqual);
                }

                blockBuilder2.Emit(OpCodes.Brfalse, unequal);
            }

            blockBuilder2.Emit(OpCodes.Ldc_I4_1);
            blockBuilder2.Emit(OpCodes.Ret);
            blockBuilder2.MarkLabel(unequal);
            blockBuilder2.Emit(OpCodes.Ldc_I4_0);
            blockBuilder2.Emit(OpCodes.Ret);

            // hashCode method:
            //   public int hashCode() {
            //     int h = 0;
            //     h = hash(h, f0);
            //     ...
            //     return h;
            //   }
            var blockBuilder3 = classDeclaration
                .DefineMethod(nameof(GetHashCode), Override, typeof(int), [])
                .GetILGenerator();
            var hParameter = blockBuilder3.DeclareLocal(typeof(int));
            blockBuilder3.Emit(OpCodes.Ldc_I4_0);
            blockBuilder3.Emit(OpCodes.Stloc, hParameter);

            for (int i = 0; i < recordFields.Length; i++)
            {
                blockBuilder3.Emit(OpCodes.Ldloc, hParameter);
                blockBuilder3.Emit(OpCodes.Ldarg_0);
                blockBuilder3.Emit(OpCodes.Ldfld, fields[i]);
                blockBuilder3.Emit(OpCodes.Call, Call(BuiltInMethod.HASH.method, [typeof(int), types[i]]));
                blockBuilder3.Emit(OpCodes.Stloc, hParameter);
            }

            blockBuilder3.Emit(OpCodes.Ldloc, hParameter);
            blockBuilder3.Emit(OpCodes.Ret);

            // compareTo method:
            //   public int compareTo(MyClass that) {
            //     int c;
            //     c = compare(this.f0, that.f0);
            //     if (c != 0) return c;
            //     ...
            //     return 0;
            //   }
            var blockBuilder4 = classDeclaration
                .DefineMethod("compareTo", Override, typeof(int), [typeof(object)])
                .GetILGenerator();
            var cParameter = blockBuilder4.DeclareLocal(typeof(int));
            var thatParameter4 = blockBuilder4.DeclareLocal(classDeclaration);
            blockBuilder4.Emit(OpCodes.Ldarg_1);
            blockBuilder4.Emit(OpCodes.Castclass, classDeclaration);
            blockBuilder4.Emit(OpCodes.Stloc, thatParameter4);

            for (int i = 0; i < recordFields.Length; i++)
            {
                MethodInfo compareCall;

                try
                {
                    var method = (recordFields[i].nullable()
                        ? BuiltInMethod.COMPARE_NULLS_LAST
                        : BuiltInMethod.COMPARE).method;
                    compareCall = Call(method, [types[i], types[i]]);
                }
                catch (NotSupportedException)
                {
                    // skips the field, as Calcite does: compareTo is generated over every field, but a record
                    // used for something other than a sort key (such as aggregate state) may hold a field
                    // with no comparison
                    continue;
                }

                var conditionalStatement = blockBuilder4.DefineLabel();
                blockBuilder4.Emit(OpCodes.Ldarg_0);
                blockBuilder4.Emit(OpCodes.Ldfld, fields[i]);
                blockBuilder4.Emit(OpCodes.Ldloc, thatParameter4);
                blockBuilder4.Emit(OpCodes.Ldfld, fields[i]);
                blockBuilder4.Emit(OpCodes.Call, compareCall);
                blockBuilder4.Emit(OpCodes.Stloc, cParameter);
                blockBuilder4.Emit(OpCodes.Ldloc, cParameter);
                blockBuilder4.Emit(OpCodes.Brfalse, conditionalStatement);
                blockBuilder4.Emit(OpCodes.Ldloc, cParameter);
                blockBuilder4.Emit(OpCodes.Ret);
                blockBuilder4.MarkLabel(conditionalStatement);
            }

            blockBuilder4.Emit(OpCodes.Ldc_I4_0);
            blockBuilder4.Emit(OpCodes.Ret);

            // toString method:
            //   public String toString() {
            //     return "{f0=" + f0
            //       + ", f1=" + f1
            //       ...
            //       + "}";
            //   }
            var blockBuilder5 = classDeclaration
                .DefineMethod(nameof(ToString), Override, typeof(string), [])
                .GetILGenerator();

            if (recordFields.Length == 0)
            {
                blockBuilder5.Emit(OpCodes.Ldstr, "{}");
            }
            else
            {
                blockBuilder5.Emit(OpCodes.Ldstr, "{");

                for (int i = 0; i < recordFields.Length; i++)
                {
                    blockBuilder5.Emit(OpCodes.Ldstr, (i == 0 ? "" : ", ") + recordFields[i].getName() + "=");
                    blockBuilder5.Emit(OpCodes.Call, Concat);
                    blockBuilder5.Emit(OpCodes.Ldarg_0);
                    blockBuilder5.Emit(OpCodes.Ldfld, fields[i]);

                    if (types[i].IsValueType)
                        blockBuilder5.Emit(OpCodes.Box, types[i]);

                    blockBuilder5.Emit(OpCodes.Call, ValueOf);
                    blockBuilder5.Emit(OpCodes.Call, Concat);
                }

                blockBuilder5.Emit(OpCodes.Ldstr, "}");
                blockBuilder5.Emit(OpCodes.Call, Concat);
            }

            blockBuilder5.Emit(OpCodes.Ret);

            return classDeclaration.CreateType();
        }

        /// <summary>
        /// Emits a constructor that assigns each parameter to the field in the same position.
        /// </summary>
        /// <remarks>
        /// The counterpart of <c>Expressions.constructorDecl</c>; with no parameters the body only calls the
        /// base constructor, as Calcite's is empty.
        /// </remarks>
        /// <param name="classDeclaration">The class being emitted.</param>
        /// <param name="parameters">The constructor's parameter types, one per field assigned.</param>
        /// <param name="fields">The fields, in the order of <paramref name="parameters"/>.</param>
        static void ConstructorDecl(TypeBuilder classDeclaration, Type[] parameters, FieldBuilder[] fields)
        {
            var constructor = classDeclaration.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, parameters);
            var il = constructor.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Call, SyntheticRecordConstructor);

            for (int i = 0; i < parameters.Length; i++)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg, i + 1);
                il.Emit(OpCodes.Stfld, fields[i]);
            }

            il.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Returns the record's fields as an array.
        /// </summary>
        /// <param name="type">The synthetic record type.</param>
        /// <returns>The record's fields, in declaration order.</returns>
        static J.Types.RecordField[] RecordFields(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            var list = type.getRecordFields();
            var fields = new J.Types.RecordField[list.size()];
            for (int i = 0; i < fields.Length; i++)
                fields[i] = (J.Types.RecordField)list.get(i);

            return fields;
        }

        /// <summary>
        /// Returns the overload of the named method that takes the given argument types.
        /// </summary>
        /// <exception cref="NotSupportedException">No overload accepts the arguments.</exception>
        /// <remarks>
        /// The counterpart of <c>Expressions.call(method.getDeclaringClass(), method.getName(), arguments)</c>,
        /// which Calcite uses for every call in these bodies. It binds through
        /// <see cref="ClrTypes.Resolve(Type, string, Type[])"/>, which matches on assignability alone; that suits
        /// IL, where nothing converts the arguments.
        /// </remarks>
        /// <param name="method">The Java method whose declaring class and name select the overloads.</param>
        /// <param name="arguments">The argument types, in order.</param>
        /// <returns>The CLR overload whose parameters the arguments are assignable to.</returns>
        static MethodInfo Call(java.lang.reflect.Method method, Type[] arguments)
        {
            return ClrTypes.Resolve(ClrTypes.FromClass(method.getDeclaringClass()), method.getName(), arguments);
        }

        /// <summary>
        /// The attributes of a method that overrides a member of the base.
        /// </summary>
        const MethodAttributes Override = MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig;

        /// <summary>
        /// The base constructor every emitted record chains to.
        /// </summary>
        static readonly ConstructorInfo SyntheticRecordConstructor = typeof(SyntheticRecord).GetConstructor(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, [])
            ?? throw new InvalidOperationException($"{nameof(SyntheticRecord)} has no no-arg constructor.");

        /// <summary>
        /// <c>BuiltInMethod.OBJECTS_EQUAL</c>, which the emitted <c>Equals</c> compares a reference field with.
        /// </summary>
        static readonly MethodInfo ObjectsEqual = ClrTypes.Resolve(BuiltInMethod.OBJECTS_EQUAL.method);

        /// <summary>
        /// String concatenation, which <c>Expressions.add</c> over strings compiles to.
        /// </summary>
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])
            ?? throw new InvalidOperationException("String has no Concat(string, string).");

        /// <summary>
        /// <c>java.util.Objects.toString</c>, standing in for <c>String.valueOf(Object)</c>, whose IKVM helper
        /// is internal. Both render a null as "null".
        /// </summary>
        static readonly MethodInfo ValueOf = typeof(java.util.Objects).GetMethod("toString", [typeof(object)])
            ?? throw new InvalidOperationException("java.util.Objects has no toString(Object).");

    }

}
