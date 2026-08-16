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
    /// <c>JavaRowFormat.CUSTOM</c> asks the type factory for the class of a row, and for a struct of several
    /// fields the answer is a synthetic record: a type Calcite describes but has not got, because Janino
    /// materialises it from the generated source.
    ///
    /// <para>Calcite never resolves one. <c>Types.toClass</c> throws on a <c>RecordType</c> — the class is
    /// still text — and <c>Expressions.parameter</c> carries the type until Janino writes its name into the
    /// same compilation unit as the class declaration. <see cref="System.Linq.Expressions.Expression"/> takes
    /// a <see cref="Type"/>, so here the class has to be real before anything can name it, which is why
    /// <see cref="ClassDecl"/> returns a finished type rather than a declaration and why all six members are
    /// on it before it returns.</para>
    /// </remarks>
    static class SyntheticRecordEmitter
    {

        static readonly object sync = new();
        static int count;

        /// <summary>
        /// The type emitted for each record type, held only for as long as the record type is.
        /// </summary>
        /// <remarks>
        /// <b>Weakly, because this is the whole of the lifetime.</b> A <c>SyntheticRecordType</c> is reachable
        /// from one place — <c>JavaTypeFactoryImpl.syntheticTypes</c>, an instance field — so it lives exactly
        /// as long as the factory that made it, which is as long as the connection. Nothing reaches back the
        /// other way: the record type holds its fields, its relational type and its name, and a field holds
        /// the record type, and none of them holds the factory. So keying on the record type is what ties the
        /// emitted type to the connection, and a strong map would untie it — the key alone would keep every
        /// record type of every connection alive for the life of the process, and the emitted type with it.
        ///
        /// <para>The emitted type does not refer to the record type, which is what makes this safe to hold in
        /// a table whose value must not reach its key.</para>
        /// </remarks>
        static readonly ConditionalWeakTable<JavaTypeFactoryImpl.SyntheticRecordType, Type> emitted = new();

        /// <summary>
        /// Returns the CLR type of a synthetic record, emitting it the first time it is asked for.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableRelImplementor.classDecl</c>, which answers a class declaration for Janino to compile.
        /// A declaration of the same shape twice is one class twice there, because the type factory hands out
        /// one <c>SyntheticRecordType</c> per shape and the declaration is written once per compilation unit;
        /// here the type is the thing itself, so it is emitted once per record type and kept.
        /// </remarks>
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
        /// Emits the class and its six members.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// The body of <c>classDecl</c>, section for section. What differs is at the leaves: a member is
        /// emitted rather than added to a list of declarations, and a body is IL rather than a linq4j block,
        /// so each statement Calcite writes as one call — <c>ifThen</c>, <c>return_</c>, <c>foldAnd</c> — is
        /// several instructions here. The order, the locals and the per field loops are Calcite's.
        /// </remarks>
        static Type Emit(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            // one collectable assembly per record type, so that the type goes when the record type does.
            // RunAndCollect collects an assembly, not a type, so a shared one is released only when every
            // type in it is unreachable -- and never at all while a static field holds the builder. Measured:
            // an assembly of its own costs about 73us more to emit and about 32KB of loader allocator while
            // the type is alive, and that 32KB comes back. A shared module returns nothing.
            var name = $"{type.getName()}_{++count}";
            var module = AssemblyBuilder
                .DefineDynamicAssembly(new AssemblyName(name), AssemblyBuilderAccess.RunAndCollect)
                .DefineDynamicModule(name);

            // the factory names a record after its shape, and two connections can each hold one of the same
            // name describing the same shape, so the name is made unique -- an assembly apiece means they
            // cannot collide, but a stack trace naming one of two Record3_0s would say nothing
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

            // Here a constructor without parameter is used because the generated
            // code could cause error if number of fields is too large.
            ConstructorDecl(classDeclaration, [], fields);

            // Calcite declares only that one, and its generated code assigns the fields. An expression tree
            // has no statements to assign them in, so JavaRowFormat.CUSTOM.record calls a constructor and the
            // one it calls is emitted for it. A record of no fields would have two that cannot be told apart;
            // a semi join whose right input projects nothing is one.
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

            // foldAnd, which short circuits: each condition falls through to the next and any false returns
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
                    // Just ignore the field in compareTo
                    // "create synthetic record class" blindly creates compareTo for
                    // all the fields, however not all the records will actually be used
                    // as sorting keys (e.g. temporary state for aggregate calculation).
                    // In those cases it is fine if we skip the problematic fields.
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
        /// Emits a constructor assigning each of its parameters to the field of the same position.
        /// </summary>
        /// <param name="classDeclaration"></param>
        /// <param name="parameters"></param>
        /// <param name="fields"></param>
        /// <remarks>
        /// <c>Expressions.constructorDecl</c>, whose body Calcite leaves empty.
        /// </remarks>
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
        /// <param name="type"></param>
        /// <returns></returns>
        static J.Types.RecordField[] RecordFields(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            var list = type.getRecordFields();
            var fields = new J.Types.RecordField[list.size()];
            for (int i = 0; i < fields.Length; i++)
                fields[i] = (J.Types.RecordField)list.get(i);

            return fields;
        }

        /// <summary>
        /// Returns the overload of the named method that takes the given arguments.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// <c>Expressions.call(method.getDeclaringClass(), method.getName(), arguments)</c>, which is how
        /// Calcite writes every call in these bodies: it names a class and a method and lets
        /// <c>Types.lookupMethod</c> bind the overload from the argument types.
        ///
        /// <para><see cref="ClrTypes.Resolve(Type, string, Type[])"/> is that method. It binds on
        /// assignability alone, which is <c>Types.assignableFrom</c> and is what a body of IL needs: nothing
        /// converts an argument here, where a tree Calcite composes converts each one to its parameter.</para>
        /// </remarks>
        static MethodInfo Call(java.lang.reflect.Method method, Type[] arguments)
        {
            return ClrTypes.Resolve(ClrTypes.FromClass(method.getDeclaringClass()), method.getName(), arguments);
        }

        /// <summary>
        /// What a member of the record overrides the base with.
        /// </summary>
        const MethodAttributes Override = MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig;

        /// <summary>
        /// The base constructor every emitted record chains to.
        /// </summary>
        static readonly ConstructorInfo SyntheticRecordConstructor = typeof(SyntheticRecord).GetConstructor(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, [])
            ?? throw new InvalidOperationException($"{nameof(SyntheticRecord)} has no no-arg constructor.");

        /// <summary>
        /// Guava's <c>Objects.equal</c>, which the generated equals compares a reference field with.
        /// </summary>
        static readonly MethodInfo ObjectsEqual = ClrTypes.Resolve(BuiltInMethod.OBJECTS_EQUAL.method);

        /// <summary>
        /// Java's string concatenation, which is what <c>Expressions.add</c> over a string is.
        /// </summary>
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string)])
            ?? throw new InvalidOperationException("String has no Concat(string, string).");

        /// <summary>
        /// <c>String.valueOf</c>, whose own helper IKVM keeps internal; both render a null as "null".
        /// </summary>
        static readonly MethodInfo ValueOf = typeof(java.util.Objects).GetMethod("toString", [typeof(object)])
            ?? throw new InvalidOperationException("java.util.Objects has no toString(Object).");

    }

}
