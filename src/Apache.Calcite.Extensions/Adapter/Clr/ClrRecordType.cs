using System;
using System.Collections.Concurrent;
using System.Reflection;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// A CLR type presented to linq4j as a record, so that its members can be reached by ordinal.
    /// </summary>
    /// <remarks>
    /// <b>Not a type.</b> The type is <see cref="ClrType"/>, and this adds nothing to it: the members are the
    /// ones <see cref="ClrReflect"/> already found. What it is, is an index — member <em>i</em> of a row is
    /// named <em>N</em> — wearing <c>java.lang.reflect.Type</c> because that is the slot linq4j has.
    ///
    /// <para>It exists because <c>JavaRowFormat.CUSTOM.field</c> has exactly two branches: a
    /// <c>Types.RecordType</c> is asked for its field by ordinal, and anything else goes to
    /// <c>Types.nthField</c>, which is <c>getFields()[ordinal]</c>. A .NET class answers nothing from
    /// <c>getFields()</c>, so the second branch throws and there is no third. Being one of these is the only
    /// way in, and it is the same shape <c>JavaTypeFactoryImpl.SyntheticRecordType</c> already has.</para>
    ///
    /// <para>Interned per CLR type, because a row type is compared and a record type is what a row expression
    /// is declared with in more than one place.</para>
    /// </remarks>
    public sealed class ClrRecordType : J.Types.RecordType
    {

        static readonly ConcurrentDictionary<Type, ClrRecordType> Cache = new();

        /// <summary>
        /// Returns the record type of a CLR type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public static ClrRecordType Of(Type type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return Cache.GetOrAdd(type, static t => new ClrRecordType(t));
        }

        readonly Type clrType;
        readonly java.util.List fields;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="clrType"></param>
        ClrRecordType(Type clrType)
        {
            this.clrType = clrType;

            // the fields name this as their declaring type, which is what sends a member access back through
            // ClrTypes.Resolve to the CLR type rather than to a Java class with no fields on it
            var members = ClrReflect.Members(clrType);
            var list = new java.util.ArrayList(members.Count);
            for (int i = 0; i < members.Count; i++)
                list.add(new ClrRecordField(this, members[i].Member, members[i].Type, members[i].Nullable));

            fields = java.util.Collections.unmodifiableList(list);
        }

        /// <summary>
        /// The CLR type a row of this is an instance of.
        /// </summary>
        public Type ClrType => clrType;

        /// <inheritdoc />
        public java.util.List getRecordFields() => fields;

        /// <inheritdoc />
        public string getName() => clrType.FullName ?? clrType.Name;

        /// <inheritdoc />
        public string getTypeName() => getName();

        /// <inheritdoc />
        public override string ToString() => getName();

    }

    /// <summary>
    /// One member of a CLR type, presented to linq4j as a field of a record.
    /// </summary>
    /// <remarks>
    /// <c>JavaTypeFactoryImpl.RecordFieldImpl</c>, over a member that exists rather than one that is going to
    /// be emitted.
    /// </remarks>
    public sealed class ClrRecordField : J.Types.RecordField
    {

        readonly ClrRecordType declaring;
        readonly MemberInfo member;
        readonly Type clrType;
        readonly bool nullability;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="declaring"></param>
        /// <param name="member"></param>
        /// <param name="clrType"></param>
        /// <param name="nullability"></param>
        internal ClrRecordField(ClrRecordType declaring, MemberInfo member, Type clrType, bool nullability)
        {
            this.declaring = declaring;
            this.member = member;
            this.clrType = clrType;
            this.nullability = nullability;
        }

        /// <summary>
        /// The property or field itself, which is what an access compiles to.
        /// </summary>
        public MemberInfo Member => member;

        /// <inheritdoc />
        public string getName() => member.Name;

        /// <inheritdoc />
        public java.lang.reflect.Type getType() => (java.lang.Class)clrType;

        /// <inheritdoc />
        /// <remarks>
        /// Every member here is public and none is static, a static one not being a column of a row.
        /// </remarks>
        public int getModifiers() => java.lang.reflect.Modifier.PUBLIC;

        /// <inheritdoc />
        public bool nullable() => nullability;

        /// <inheritdoc />
        /// <remarks>
        /// <c>RecordFieldImpl</c> throws here, and rightly: a synthetic record has no member to read, the
        /// class not existing yet. This one does, so it answers. Nothing on the plan path calls it —
        /// <c>OptimizeShuttle</c> reaches <c>PseudoField.get</c> only for a constant receiver — but refusing
        /// would be copying an admission of absence rather than a behaviour.
        /// </remarks>
        public object? get(object? o)
        {
            return member switch
            {
                PropertyInfo property => property.GetValue(o),
                FieldInfo field => field.GetValue(o),
                _ => throw new NotSupportedException($"'{member}' is neither a property nor a field.")
            };
        }

        /// <inheritdoc />
        public java.lang.reflect.Type getDeclaringClass() => declaring;

        /// <inheritdoc />
        public override string ToString() => $"{declaring.getName()}.{member.Name}";

    }

}
