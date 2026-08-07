using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.jdbc;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Emits the CLR type behind a <see cref="JavaTypeFactoryImpl.SyntheticRecordType"/>.
    /// </summary>
    /// <remarks>
    /// <c>JavaRowFormat.CUSTOM</c> asks the type factory for the class of a row, and for a struct of several
    /// fields the answer is a synthetic record: a type Calcite describes but has not got, because Janino
    /// materialises it from the generated source. Nothing here changes what a row is; the record's fields and
    /// their types are the factory's, and this only gives them somewhere to live.
    ///
    /// <para>Calcite writes equals, hashCode, compareTo and toString into the generated class. Those are on
    /// <see cref="SyntheticRecord"/> instead, built from the fields once per type, so what is emitted is the
    /// fields and the two constructors and nothing else.</para>
    /// </remarks>
    static class SyntheticRecordEmitter
    {

        static readonly object sync = new();
        static readonly Dictionary<JavaTypeFactoryImpl.SyntheticRecordType, Type> emitted = new(ReferenceEqualityComparer.Instance);
        static ModuleBuilder? module;
        static int count;

        /// <summary>
        /// Returns the CLR type for a synthetic record, emitting it the first time it is asked for.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Type Emit(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            ArgumentNullException.ThrowIfNull(type);

            lock (sync)
            {
                if (emitted.TryGetValue(type, out var existing))
                    return existing;

                return emitted[type] = Build(type);
            }
        }

        /// <summary>
        /// Emits the type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        static Type Build(JavaTypeFactoryImpl.SyntheticRecordType type)
        {
            module ??= AssemblyBuilder
                .DefineDynamicAssembly(new AssemblyName("Apache.Calcite.Extensions.Records"), AssemblyBuilderAccess.RunAndCollect)
                .DefineDynamicModule("Apache.Calcite.Extensions.Records");

            // the factory names a record after its shape, and two connections can each hold one of the same
            // name describing the same shape, so the name is made unique rather than trusted
            var builder = module.DefineType(
                $"{type.getName()}_{++count}",
                TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.Class,
                typeof(SyntheticRecord));

            var recordFields = type.getRecordFields();
            var fields = new FieldBuilder[recordFields.size()];
            var types = new Type[recordFields.size()];

            for (int i = 0; i < fields.Length; i++)
            {
                var field = (J.Types.RecordField)recordFields.get(i);
                types[i] = ClrTypes.Resolve(field.getType());
                fields[i] = builder.DefineField(field.getName(), types[i], FieldAttributes.Public);
            }

            EmitConstructor(builder, [], fields);

            // a record of no fields has the same two constructors, and emitting both leaves the type with two
            // that cannot be told apart. A semi join whose right input projects nothing is one
            if (types.Length > 0)
                EmitConstructor(builder, types, fields);

            return builder.CreateType();
        }

        /// <summary>
        /// Emits a constructor assigning each of its parameters to the field of the same position.
        /// </summary>
        /// <param name="builder"></param>
        /// <param name="parameters"></param>
        /// <param name="fields"></param>
        /// <remarks>
        /// Both are emitted because Calcite builds a record both ways. <c>JavaRowFormat.CUSTOM.record</c>
        /// writes <c>new Foo(a, b)</c>, while <c>EnumerableAggregate</c> writes <c>new Foo()</c> and assigns
        /// each field, which it changed to under CALCITE-1097 because Janino fails on a constructor with too
        /// many parameters. Calcite's own generated class kept only the second, so the first would not have
        /// compiled against it.
        /// </remarks>
        static void EmitConstructor(TypeBuilder builder, Type[] parameters, FieldBuilder[] fields)
        {
            var constructor = builder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, parameters);
            var il = constructor.GetILGenerator();

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Call, typeof(SyntheticRecord).GetConstructor(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance, [])!);

            for (int i = 0; i < parameters.Length; i++)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg, i + 1);
                il.Emit(OpCodes.Stfld, fields[i]);
            }

            il.Emit(OpCodes.Ret);
        }

    }

}
