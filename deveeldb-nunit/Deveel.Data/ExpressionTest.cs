using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ExpressionTest {
		[Test]
		public void SimpleExpression() {
			Expression exp = Expression.Parse("a = b");
			Assert.IsTrue(exp[0].Equals(Variable.Resolve("a")));
			Assert.IsTrue(exp[1].Equals(Variable.Resolve("b")));
			Assert.IsTrue(exp[2] == Operator.Get("="));

			exp = Expression.Parse("a=Table.Column");
			Assert.AreEqual(exp[0], Variable.Resolve("a"));
			Assert.AreEqual(exp[1], Variable.Resolve(TableName.Resolve("Table"), "Column"));
			Assert.IsTrue(exp[2] == Operator.Get("="));

			exp = Expression.Parse("b > 35");
			Assert.AreEqual(exp[0], Variable.Resolve("b"));
			Assert.IsTrue(((TObject)exp[1]).ValuesEqual(TObject.GetInt4(35)));

			exp = Expression.Parse("47");
			Assert.IsTrue(exp.IsConstant);
			Assert.IsTrue(((TObject)exp[0]).ValuesEqual(TObject.GetInt4(47)));

			exp = Expression.Parse("a * (b + c)");
			Assert.AreEqual(exp[0], Variable.Resolve("a"));
			Assert.AreEqual(exp[1], Variable.Resolve("b"));
			Assert.AreEqual(exp[2], Variable.Resolve("c"));
			Assert.AreEqual(exp[3], Operator.Get("+"));
			Assert.AreEqual(exp[4], Operator.Get("*"));

			exp = Expression.Parse("34 + 159");
			Assert.IsTrue(((TObject)exp[0]).ValuesEqual(TObject.GetInt4(34)));
			Assert.IsTrue(((TObject)exp[1]).ValuesEqual(TObject.GetInt4(159)));
			Assert.AreEqual(exp[2], Operator.Get("+"));
			TObject result = exp.Evaluate(null, null);
			Assert.IsTrue(exp.IsConstant);
			Assert.IsTrue(result.ValuesEqual(TObject.GetInt4(193)));
		}

		[Test]
		public void FunctionExpression() {
			Expression exp = Expression.Parse("LENGTH(a)");
			Assert.IsTrue(exp[0] is Functions.FunctionDef);
			Functions.FunctionDef fdef = (Functions.FunctionDef) exp[0];
			Assert.AreEqual(fdef.Name, "LENGTH");
			Assert.AreEqual(fdef.Parameters.Length, 1);
			Assert.AreEqual(fdef.Parameters[0].AllVariables.Count, 1);
			Assert.AreEqual(fdef.Parameters[0].AllVariables[0], Variable.Resolve("a"));

			exp = Expression.Parse("LENGTH('test')");
			fdef = (Functions.FunctionDef)exp[0];
			Assert.AreEqual(fdef.Name, "LENGTH");
			Assert.AreEqual(fdef.Parameters.Length, 1);
			Assert.AreEqual(fdef.Parameters[0].AllElements.Count, 1);
			TObject result = fdef.GetFunction(null).Evaluate(null, null, null);
			Assert.IsTrue(result == 4);

			exp = Expression.Parse("LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR))");
			fdef = (Functions.FunctionDef) exp[0];
			result = fdef.GetFunction(null).Evaluate(null, null, null);
		}

		[Test]
		public void ParameterSubst() {
			Expression exp = Expression.Parse("?");
			Assert.IsTrue(exp[0] is ParameterSubstitution);
			Assert.AreEqual(((ParameterSubstitution) exp[0]).Id, 0);

			exp = Expression.Parse("@Param");
			Assert.IsTrue(exp[0] is ParameterSubstitution);
			Assert.AreEqual(((ParameterSubstitution)exp[0]).Name, "@Param");
		}
	}
}