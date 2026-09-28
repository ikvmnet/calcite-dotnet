using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.runtime;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Rel.Metadata
{

    /// <summary>
    /// Emits the class behind a <see cref="MetadataHandler"/>.
    /// </summary>
    /// <remarks>
    /// Emits as IL the class <c>JaninoRelMetadataProvider</c> generates as Java source: a field per underlying
    /// handler, a public method per handler method implementing the cache protocol, a private method beneath
    /// each holding the <c>instanceof</c> dispatch, and <c>getDef</c> returning the first handler's. A
    /// metadata call is therefore a direct call to the handler's method, with no delegate or argument array.
    ///
    /// <para>Two details differ from the generated Java. The cache keys and lookup tables are held in one
    /// array passed to the constructor, since an emitted method cannot hold object constants. And the cache
    /// protocol catches <see cref="Exception"/> where Calcite catches <c>java.lang.Exception</c>, because
    /// IKVM's mapping of CLR exceptions into the Java hierarchy is not available to raw IL; so the cache row
    /// is also cleared for a Java <c>Error</c> and for a CLR exception with no Java counterpart, where
    /// Calcite would leave it.</para>
    /// </remarks>
    static class ClrMetadataHandlerEmitter
    {

        static readonly MethodInfo TableGet = typeof(com.google.common.collect.Table).GetMethod("get", [typeof(object), typeof(object)])!;
        static readonly MethodInfo TablePut = typeof(com.google.common.collect.Table).GetMethod("put", [typeof(object), typeof(object), typeof(object)])!;
        static readonly MethodInfo TableRow = typeof(com.google.common.collect.Table).GetMethod("row", [typeof(object)])!;
        static readonly MethodInfo MapClear = typeof(java.util.Map).GetMethod("clear", Type.EmptyTypes)!;
        static readonly MethodInfo Mask = typeof(NullSentinel).GetMethod("mask", [typeof(object)])!;
        static readonly MethodInfo Delegate = typeof(DelegatingMetadataRel).GetMethod("getMetadataDelegateRel", Type.EmptyTypes)!;
        static readonly MethodInfo Ordinal = typeof(java.lang.Enum).GetMethod("ordinal", Type.EmptyTypes)!;
        static readonly MethodInfo GetDef = typeof(MetadataHandler).GetMethod("getDef", Type.EmptyTypes)!;
        static readonly MethodInfo IntegerValueOf = typeof(java.lang.Integer).GetMethod("valueOf", [typeof(int)])!;
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(string), typeof(string), typeof(string)])!;
        static readonly MethodInfo GetTypeOf = typeof(object).GetMethod(nameof(object.GetType), Type.EmptyTypes)!;
        static readonly MethodInfo ToStringOf = typeof(object).GetMethod(nameof(object.ToString), Type.EmptyTypes)!;
        static readonly ConstructorInfo Cyclic = typeof(CyclicMetadataException).GetConstructor(Type.EmptyTypes)!;
        static readonly ConstructorInfo IllegalArgument = typeof(java.lang.IllegalArgumentException).GetConstructor([typeof(string)])!;

        static readonly object sync = new();
        static readonly HashSet<string> reachable = [];
        static AssemblyBuilder? assembly;
        static ModuleBuilder? module;
        static ConstructorInfo? ignoresAccessChecks;
        static int count;

        /// <summary>
        /// Emits and instantiates a handler of <paramref name="handlerInterface"/> that implements
        /// <paramref name="methods"/> by dispatching to <paramref name="handlers"/>.
        /// </summary>
        /// <param name="handlerInterface">The handler interface.</param>
        /// <param name="methods">Its methods, in the order Calcite indexes them.</param>
        /// <param name="handlers">The underlying handlers, in priority order.</param>
        /// <returns>The emitted handler.</returns>
        public static MetadataHandler Emit(Type handlerInterface, MethodInfo[] methods, IReadOnlyList<MetadataHandler> handlers)
        {
            ArgumentNullException.ThrowIfNull(handlerInterface);
            ArgumentNullException.ThrowIfNull(methods);
            ArgumentNullException.ThrowIfNull(handlers);

            var plans = methods.Select(ClrMetadataCacheKey.Of).ToArray();
            var targets = methods.Select(m => ClrMetadataTargets.Of(m, handlers)).ToArray();

            // every cache key and lookup table, in one array, since an emitted method cannot hold an object
            // constant
            var constants = new List<object>();
            var slots = plans.Select(p => p.Constants.Select(c => { constants.Add(c); return constants.Count - 1; }).ToArray()).ToArray();

            lock (sync)
            {
                var type = Build(handlerInterface, methods, plans, slots, targets, handlers);

                // the two arrays wrapped in one, since two bare object[] arguments bind to the overload taking
                // activation attributes
                object[] arguments = [handlers.Cast<object>().ToArray(), constants.ToArray()];
                return (MetadataHandler)Activator.CreateInstance(type, arguments)!;
            }
        }

        /// <summary>
        /// Emits the type.
        /// </summary>
        static Type Build(
            Type handlerInterface,
            MethodInfo[] methods,
            ClrMetadataCacheKey.Plan[] plans,
            int[][] slots,
            ClrMetadataTargets.Target[][] targets,
            IReadOnlyList<MetadataHandler> handlers)
        {
            if (module is null)
            {
                assembly = AssemblyBuilder.DefineDynamicAssembly(new AssemblyName("Apache.Calcite.Extensions.Rel.Metadata"), AssemblyBuilderAccess.RunAndCollect);
                module = assembly.DefineDynamicModule("Apache.Calcite.Extensions.Rel.Metadata");
                ignoresAccessChecks = EmitIgnoresAccessChecksTo(module);
            }

            // some of Calcite's handlers are private nested classes, which the generated Java names and calls
            // directly because Janino does not check access; the emitted assembly skips access checks likewise
            foreach (var handler in handlers)
                AllowAccessTo(handler.GetType());
            foreach (var target in targets.SelectMany(t => t))
            {
                AllowAccessTo(target.RelClass);
                AllowAccessTo(target.Method.DeclaringType!);
            }

            // one class per provider and handler interface, so the counter keeps same-shaped names distinct
            var builder = module.DefineType(
                $"GeneratedMetadata_{handlerInterface.DeclaringType?.Name ?? handlerInterface.Name}_{++count}",
                TypeAttributes.Public | TypeAttributes.Sealed | TypeAttributes.Class,
                typeof(object),
                [handlerInterface]);

            var providers = handlers
                .Select((h, i) => builder.DefineField($"provider{i}", h.GetType(), FieldAttributes.Public | FieldAttributes.InitOnly))
                .ToArray();
            var constants = builder.DefineField("constants", typeof(object[]), FieldAttributes.Private | FieldAttributes.InitOnly);

            EmitConstructor(builder, providers, constants);

            for (int i = 0; i < methods.Length; i++)
            {
                var dispatch = EmitDispatchMethod(builder, methods[i], targets[i], providers);
                EmitCachedMethod(builder, methods[i], dispatch, plans[i], slots[i], constants);
            }

            EmitGetDef(builder, providers.FirstOrDefault());

            return builder.CreateType();
        }

        /// <summary>
        /// Defines <c>System.Runtime.CompilerServices.IgnoresAccessChecksToAttribute</c> in the emitted
        /// module; the runtime honours it but the class library does not declare it.
        /// </summary>
        static ConstructorInfo EmitIgnoresAccessChecksTo(ModuleBuilder module)
        {
            var builder = module.DefineType("System.Runtime.CompilerServices.IgnoresAccessChecksToAttribute", TypeAttributes.Public, typeof(Attribute));
            var constructor = builder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, [typeof(string)]);

            var il = constructor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Call, typeof(Attribute).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, Type.EmptyTypes, null)!);
            il.Emit(OpCodes.Ret);

            return builder.CreateType().GetConstructor([typeof(string)])!;
        }

        /// <summary>
        /// Lets the emitted assembly access non-public types of the assembly <paramref name="type"/> is in.
        /// </summary>
        static void AllowAccessTo(Type type)
        {
            if (type.Assembly.GetName().Name is string name && reachable.Add(name))
                assembly!.SetCustomAttribute(new CustomAttributeBuilder(ignoresAccessChecks!, [name]));
        }

        /// <summary>
        /// Emits the constructor, which takes the handlers and the constants.
        /// </summary>
        static void EmitConstructor(TypeBuilder builder, FieldBuilder[] providers, FieldBuilder constants)
        {
            var constructor = builder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, [typeof(object[]), typeof(object[])]);
            var il = constructor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Call, typeof(object).GetConstructor(Type.EmptyTypes)!);

            for (int i = 0; i < providers.Length; i++)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Ldc_I4, i);
                il.Emit(OpCodes.Ldelem_Ref);
                il.Emit(OpCodes.Castclass, providers[i].FieldType);
                il.Emit(OpCodes.Stfld, providers[i]);
            }

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldarg_2);
            il.Emit(OpCodes.Stfld, constants);
            il.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Emits the public method: the cache protocol around a call of the dispatch method.
        /// </summary>
        static void EmitCachedMethod(
            TypeBuilder builder,
            MethodInfo declared,
            MethodBuilder dispatch,
            ClrMetadataCacheKey.Plan plan,
            int[] slots,
            FieldBuilder constants)
        {
            var types = declared.GetParameters().Select(p => p.ParameterType).ToArray();

            // the cache protocol returns null for a cached null, which a value type cannot hold
            if (declared.ReturnType.IsValueType)
                throw new NotSupportedException($"'{declared}' returns a value type, which the cache protocol cannot answer null for.");

            var method = builder.DefineMethod(
                declared.Name,
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.NewSlot | MethodAttributes.HideBySig,
                declared.ReturnType,
                types);

            var il = method.GetILGenerator();
            var key = il.DeclareLocal(typeof(object));
            var v = il.DeclareLocal(typeof(object));
            var x = il.DeclareLocal(declared.ReturnType);

            // while (r instanceof DelegatingMetadataRel) r = ((DelegatingMetadataRel) r).getMetadataDelegateRel();
            var loop = il.DefineLabel();
            var unwound = il.DefineLabel();
            il.MarkLabel(loop);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Isinst, typeof(DelegatingMetadataRel));
            il.Emit(OpCodes.Brfalse, unwound);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Castclass, typeof(DelegatingMetadataRel));
            il.Emit(OpCodes.Callvirt, Delegate);
            il.Emit(OpCodes.Starg_S, (byte)1);
            il.Emit(OpCodes.Br, loop);
            il.MarkLabel(unwound);

            EmitKey(il, declared, plan, slots, constants);
            il.Emit(OpCodes.Stloc, key);

            // final Object v = mq.map.get(r, key);
            EmitMap(il);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldloc, key);
            il.Emit(OpCodes.Callvirt, TableGet);
            il.Emit(OpCodes.Stloc, v);

            var miss = il.DefineLabel();
            il.Emit(OpCodes.Ldloc, v);
            il.Emit(OpCodes.Brfalse, miss);

            var notActive = il.DefineLabel();
            il.Emit(OpCodes.Ldloc, v);
            EmitSentinel(il, nameof(NullSentinel.ACTIVE));
            il.Emit(OpCodes.Bne_Un, notActive);
            il.Emit(OpCodes.Newobj, Cyclic);
            il.Emit(OpCodes.Throw);
            il.MarkLabel(notActive);

            var notNull = il.DefineLabel();
            il.Emit(OpCodes.Ldloc, v);
            EmitSentinel(il, nameof(NullSentinel.INSTANCE));
            il.Emit(OpCodes.Bne_Un, notNull);
            il.Emit(OpCodes.Ldnull);
            il.Emit(OpCodes.Ret);
            il.MarkLabel(notNull);

            il.Emit(OpCodes.Ldloc, v);
            il.Emit(OpCodes.Castclass, declared.ReturnType);
            il.Emit(OpCodes.Ret);

            il.MarkLabel(miss);

            // mq.map.put(r, key, NullSentinel.ACTIVE);
            EmitMap(il);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldloc, key);
            EmitSentinel(il, nameof(NullSentinel.ACTIVE));
            il.Emit(OpCodes.Callvirt, TablePut);
            il.Emit(OpCodes.Pop);

            il.BeginExceptionBlock();

            il.Emit(OpCodes.Ldarg_0);
            for (int i = 1; i <= types.Length; i++)
                il.Emit(OpCodes.Ldarg_S, (byte)i);
            il.Emit(OpCodes.Call, dispatch);
            il.Emit(OpCodes.Stloc, x);

            EmitMap(il);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Ldloc, key);
            il.Emit(OpCodes.Ldloc, x);
            il.Emit(OpCodes.Call, Mask);
            il.Emit(OpCodes.Callvirt, TablePut);
            il.Emit(OpCodes.Pop);

            il.BeginCatchBlock(typeof(Exception));
            il.Emit(OpCodes.Pop);
            EmitMap(il);
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Callvirt, TableRow);
            il.Emit(OpCodes.Callvirt, MapClear);
            il.Emit(OpCodes.Rethrow);
            il.EndExceptionBlock();

            il.Emit(OpCodes.Ldloc, x);
            il.Emit(OpCodes.Ret);

            builder.DefineMethodOverride(method, declared);
        }

        /// <summary>
        /// Emits the private dispatch method, which tests the rel's class against each target in order and
        /// calls the matching handler.
        /// </summary>
        static MethodBuilder EmitDispatchMethod(TypeBuilder builder, MethodInfo declared, ClrMetadataTargets.Target[] targets, FieldBuilder[] providers)
        {
            var types = declared.GetParameters().Select(p => p.ParameterType).ToArray();
            var method = builder.DefineMethod(
                declared.Name + "_",
                MethodAttributes.Private | MethodAttributes.HideBySig,
                declared.ReturnType,
                types);

            var il = method.GetILGenerator();

            foreach (var target in targets)
            {
                var next = il.DefineLabel();
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Isinst, target.RelClass);
                il.Emit(OpCodes.Brfalse, next);

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, providers[target.Provider]);
                il.Emit(OpCodes.Ldarg_1);
                il.Emit(OpCodes.Castclass, target.RelClass);
                for (int i = 2; i <= types.Length; i++)
                    il.Emit(OpCodes.Ldarg_S, (byte)i);
                il.Emit(OpCodes.Callvirt, target.Method);
                il.Emit(OpCodes.Ret);

                il.MarkLabel(next);
            }

            il.Emit(OpCodes.Ldstr, $"No handler for method [{declared}] applied to argument of type [");
            il.Emit(OpCodes.Ldarg_1);
            il.Emit(OpCodes.Callvirt, GetTypeOf);
            il.Emit(OpCodes.Callvirt, ToStringOf);
            il.Emit(OpCodes.Ldstr, "]; we recommend you create a catch-all (RelNode) handler");
            il.Emit(OpCodes.Call, Concat);
            il.Emit(OpCodes.Newobj, IllegalArgument);
            il.Emit(OpCodes.Throw);

            return method;
        }

        /// <summary>
        /// Emits <c>getDef</c>, which returns the first handler's.
        /// </summary>
        static void EmitGetDef(TypeBuilder builder, FieldBuilder? provider)
        {
            var method = builder.DefineMethod(
                GetDef.Name,
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.Final | MethodAttributes.NewSlot | MethodAttributes.HideBySig,
                GetDef.ReturnType,
                Type.EmptyTypes);

            var il = method.GetILGenerator();
            if (provider is null)
            {
                il.Emit(OpCodes.Ldnull);
            }
            else
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, provider);
                il.Emit(OpCodes.Callvirt, GetDef);
            }

            il.Emit(OpCodes.Ret);
            builder.DefineMethodOverride(method, GetDef);
        }

        /// <summary>
        /// Emits the block computing the cache key, leaving it on the stack.
        /// </summary>
        static void EmitKey(ILGenerator il, MethodInfo declared, ClrMetadataCacheKey.Plan plan, int[] slots, FieldBuilder constants)
        {
            void Constant(int slot)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, constants);
                il.Emit(OpCodes.Ldc_I4, slot);
                il.Emit(OpCodes.Ldelem_Ref);
            }

            switch (plan.Kind)
            {
                case ClrMetadataCacheKey.Strategy.NoArg:
                    Constant(slots[0]);
                    break;

                case ClrMetadataCacheKey.Strategy.Boolean:
                {
                    var no = il.DefineLabel();
                    var done = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Brfalse, no);
                    Constant(slots[0]);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(no);
                    Constant(slots[1]);
                    il.MarkLabel(done);
                    break;
                }

                case ClrMetadataCacheKey.Strategy.Enum:
                {
                    var some = il.DefineLabel();
                    var done = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Brtrue, some);
                    Constant(slots[0]);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(some);
                    Constant(slots[1]);
                    il.Emit(OpCodes.Castclass, typeof(object[]));
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Callvirt, Ordinal);
                    il.Emit(OpCodes.Ldelem_Ref);
                    il.MarkLabel(done);
                    break;
                }

                case ClrMetadataCacheKey.Strategy.Int:
                {
                    var wide = il.DefineLabel();
                    var done = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Ldc_I4, ClrMetadataCacheKey.Min);
                    il.Emit(OpCodes.Blt, wide);
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Ldc_I4, ClrMetadataCacheKey.Max);
                    il.Emit(OpCodes.Bge, wide);
                    Constant(slots[1]);
                    il.Emit(OpCodes.Castclass, typeof(object[]));
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Ldc_I4, -ClrMetadataCacheKey.Min);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Ldelem_Ref);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(wide);
                    Constant(slots[0]);
                    il.Emit(OpCodes.Ldarg_S, (byte)3);
                    il.Emit(OpCodes.Call, IntegerValueOf);
                    il.Emit(OpCodes.Call, ListOf(2));
                    il.MarkLabel(done);
                    break;
                }

                case ClrMetadataCacheKey.Strategy.List:
                {
                    var parameters = declared.GetParameters();
                    Constant(slots[0]);

                    for (int i = 2; i < parameters.Length; i++)
                        EmitSafeArgument(il, (byte)(i + 1), parameters[i].ParameterType);

                    il.Emit(OpCodes.Call, ListOf(parameters.Length - 1));
                    break;
                }

                default:
                    throw new NotSupportedException($"'{plan.Kind}' is not a cache key strategy.");
            }
        }

        /// <summary>
        /// Emits one argument as the key list holds it.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>safeArgList</c>: a primitive or a <c>RexNode</c> is added as it is, and anything else
        /// through <c>NullSentinel.mask</c>, because the list cannot hold null. A primitive is boxed as Java
        /// boxes it (<c>Integer.valueOf</c>), not as the CLR does.
        /// </remarks>
        static void EmitSafeArgument(ILGenerator il, byte argument, Type type)
        {
            il.Emit(OpCodes.Ldarg_S, argument);

            if (J.Primitive.@is((java.lang.Class)type))
            {
                var box = Apache.Calcite.Extensions.Linq4j.Tree.ClrTypes.FromClass(J.Primitive.of((java.lang.Class)type).boxClass);
                il.Emit(OpCodes.Call, box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [type], null)!);
                return;
            }

            if (typeof(RexNode).IsAssignableFrom(type))
                return;

            il.Emit(OpCodes.Call, Mask);
        }

        /// <summary>
        /// Returns the list factory Calcite builds a key of <paramref name="arity"/> elements with.
        /// </summary>
        static MethodInfo ListOf(int arity)
        {
            // as in Calcite, ImmutableList from five elements (a method of six parameters); no current
            // metadata method is that wide, but the list class would differ
            var declaring = arity < 5 ? typeof(FlatLists) : typeof(com.google.common.collect.ImmutableList);

            return declaring.GetMethod("of", Enumerable.Repeat(typeof(object), arity).ToArray())
                ?? throw new NotSupportedException($"'{declaring}' has no of taking {arity} objects.");
        }

        /// <summary>
        /// Emits a read of the query's cache table, the Java field <c>RelMetadataQueryBase.map</c>.
        /// </summary>
        static void EmitMap(ILGenerator il)
        {
            il.Emit(OpCodes.Ldarg_2);
            EmitRead(il, typeof(RelMetadataQueryBase), "map");
        }

        /// <summary>
        /// Emits a read of one of the <c>NullSentinel</c> enum constants.
        /// </summary>
        static void EmitSentinel(ILGenerator il, string name)
        {
            EmitRead(il, typeof(NullSentinel), name);
        }

        /// <summary>
        /// Emits a read of a Java field; for an instance field, the target is already on the stack.
        /// </summary>
        /// <remarks>
        /// IKVM compiles a Java <c>static final</c> field as a property over a renamed backing field, so that
        /// reading it runs the class initializer; where there is no field of the name, the property is read.
        /// </remarks>
        static void EmitRead(ILGenerator il, Type declaring, string name)
        {
            const BindingFlags All = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;

            if (declaring.GetField(name, All) is FieldInfo field)
            {
                il.Emit(field.IsStatic ? OpCodes.Ldsfld : OpCodes.Ldfld, field);
                return;
            }

            if (declaring.GetProperty(name, All)?.GetMethod is MethodInfo getter)
            {
                il.Emit(getter.IsStatic ? OpCodes.Call : OpCodes.Callvirt, getter);
                return;
            }

            throw new NotSupportedException($"'{declaring}' has no field or property '{name}'.");
        }

    }

}
