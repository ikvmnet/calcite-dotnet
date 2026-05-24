using System;
using System.Reflection;
using System.Reflection.Emit;

using Apache.Calcite.Linq4j;

using FastExpressionCompiler.LightExpression;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.linq4j.tree;

using LightExpr = FastExpressionCompiler.LightExpression.Expression;

namespace Apache.Calcite.Data.Tests
{

    [TestClass]
    public class Linq4jExpressionTranslatorTests
    {

        // ── Shared infrastructure ─────────────────────────────────────────────────────────────────────────────────────

        static readonly ModuleBuilder s_module = AssemblyBuilder
            .DefineDynamicAssembly(new AssemblyName("Linq4jTranslatorTests"), AssemblyBuilderAccess.Run)
            .DefineDynamicModule("Main");

        static Linq4jExpressionTranslator Translator() => new(s_module);

        // Shorthand: CLR Type → java.lang.Class (IKVM supports direct explicit cast)
        static java.lang.Class JClass(Type clrType) => (java.lang.Class)clrType;

        /// <summary>Translates a linq4j <paramref name="node"/>, wraps the result in a no-arg compiled lambda, and returns the invocation result.</summary>
        static T Eval<T>(Node node)
        {
            var t = Translator();
            t.ResetScope();
            var light = t.Translate(node);
            var lambda = LightExpr.Lambda<Func<T>>(light);
            return lambda.CompileFast()();
        }

        // ── ConstantExpression ────────────────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Constant_Int_RoundTrips()
        {
            var node = Expressions.constant(42);
            Assert.AreEqual(42, Eval<int>(node));
        }

        [TestMethod]
        public void Constant_String_RoundTrips()
        {
            var node = Expressions.constant("hello");
            Assert.AreEqual("hello", Eval<string>(node));
        }

        [TestMethod]
        public void Constant_Null_ProducesNull()
        {
            var node = Expressions.constant(null, JClass(typeof(string)));
            Assert.IsNull(Eval<string>(node));
        }

        // ── BinaryExpression ──────────────────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Binary_Add_Int()
        {
            var node = Expressions.add(Expressions.constant(3), Expressions.constant(4));
            Assert.AreEqual(7, Eval<int>(node));
        }

        [TestMethod]
        public void Binary_Subtract_Int()
        {
            var node = Expressions.subtract(Expressions.constant(10), Expressions.constant(3));
            Assert.AreEqual(7, Eval<int>(node));
        }

        [TestMethod]
        public void Binary_Multiply_Int()
        {
            var node = Expressions.multiply(Expressions.constant(3), Expressions.constant(4));
            Assert.AreEqual(12, Eval<int>(node));
        }

        [TestMethod]
        public void Binary_Equal_True()
        {
            var node = Expressions.equal(Expressions.constant(5), Expressions.constant(5));
            Assert.IsTrue(Eval<bool>(node));
        }

        [TestMethod]
        public void Binary_Equal_False()
        {
            var node = Expressions.equal(Expressions.constant(5), Expressions.constant(6));
            Assert.IsFalse(Eval<bool>(node));
        }

        [TestMethod]
        public void Binary_LessThan()
        {
            var node = Expressions.lessThan(Expressions.constant(3), Expressions.constant(5));
            Assert.IsTrue(Eval<bool>(node));
        }

        [TestMethod]
        public void Binary_AndAlso_ShortCircuits()
        {
            // false && true → false
            var node = Expressions.andAlso(Expressions.constant(false), Expressions.constant(true));
            Assert.IsFalse(Eval<bool>(node));
        }

        [TestMethod]
        public void Binary_OrElse_ShortCircuits()
        {
            // true || false → true
            var node = Expressions.orElse(Expressions.constant(true), Expressions.constant(false));
            Assert.IsTrue(Eval<bool>(node));
        }

        // ── UnaryExpression ───────────────────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Unary_Negate_Int()
        {
            var node = Expressions.negate(Expressions.constant(7));
            Assert.AreEqual(-7, Eval<int>(node));
        }

        [TestMethod]
        public void Unary_Not_Bool()
        {
            var node = Expressions.not(Expressions.constant(true));
            Assert.IsFalse(Eval<bool>(node));
        }

        [TestMethod]
        public void Unary_Convert_IntToDouble()
        {
            var node = Expressions.convert_(Expressions.constant(3), JClass(typeof(double)));
            Assert.AreEqual(3.0, Eval<double>(node));
        }

        // ── TernaryExpression (Condition) ─────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Ternary_TrueBranch()
        {
            var node = Expressions.condition(Expressions.constant(true), Expressions.constant(1), Expressions.constant(2));
            Assert.AreEqual(1, Eval<int>(node));
        }

        [TestMethod]
        public void Ternary_FalseBranch()
        {
            var node = Expressions.condition(Expressions.constant(false), Expressions.constant(1), Expressions.constant(2));
            Assert.AreEqual(2, Eval<int>(node));
        }

        // ── BlockStatement ────────────────────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Block_MultipleStatements_LastValueReturned()
        {
            // { int x = 1; x + 41; }  → the block value is the last expression
            var paramNode = Expressions.parameter(JClass(typeof(int)), "x");
            var decl = Expressions.declare(0, paramNode, Expressions.constant(1));
            var add = Expressions.add(paramNode, Expressions.constant(41));
            var stmts = java.util.Arrays.asList(new java.lang.Object[] { decl, add });
            var block = Expressions.block(JClass(typeof(int)), stmts);

            Assert.AreEqual(42, Eval<int>(block));
        }

        // ── FunctionExpression / Lambda ───────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void Lambda_IdentityInt_CompilesAndRuns()
        {
            var p = Expressions.parameter(JClass(typeof(int)), "x");

            var t  = Translator();
            t.ResetScope();
            var lp       = t.GetOrCreateParameter(p);
            var lBody    = t.Translate(p);
            var compiled = LightExpr.Lambda<Func<int, int>>(lBody, lp).CompileFast();
            Assert.AreEqual(99, compiled(99));
        }

        [TestMethod]
        public void Lambda_AddTwo_CompilesAndRuns()
        {
            var p    = Expressions.parameter(JClass(typeof(int)), "x");
            var body = Expressions.add(p, Expressions.constant(2));

            var t  = Translator();
            t.ResetScope();
            var lp       = t.GetOrCreateParameter(p);
            var lBody    = t.Translate(body);
            var compiled = LightExpr.Lambda<Func<int, int>>(lBody, lp).CompileFast();
            Assert.AreEqual(7, compiled(5));
        }

        [TestMethod]
        public void Lambda_MultiParam_CompilesAndRuns()
        {
            var a    = Expressions.parameter(JClass(typeof(int)), "a");
            var b    = Expressions.parameter(JClass(typeof(int)), "b");
            var body = Expressions.multiply(a, b);

            var t  = Translator();
            t.ResetScope();
            var la       = t.GetOrCreateParameter(a);
            var lb       = t.GetOrCreateParameter(b);
            var lBody    = t.Translate(body);
            var compiled = LightExpr.Lambda<Func<int, int, int>>(lBody, la, lb).CompileFast();
            Assert.AreEqual(20, compiled(4, 5));
        }

        // ── ClrType helper ────────────────────────────────────────────────────────────────────────────────────────────

        [TestMethod]
        public void ClrType_IntClass_ReturnsDotNetInt()
        {
            Assert.AreEqual(typeof(int), Linq4jExpressionTranslator.ClrType(JClass(typeof(int))));
        }

        [TestMethod]
        public void ClrType_StringClass_ReturnsDotNetString()
        {
            Assert.AreEqual(typeof(string), Linq4jExpressionTranslator.ClrType(JClass(typeof(string))));
        }

    }

}
