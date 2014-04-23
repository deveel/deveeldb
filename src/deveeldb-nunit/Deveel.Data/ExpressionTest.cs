using System;
using System.Collections.Generic;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ExpressionTest {
		[Test]
		public void SimpleExpression() {
			Expression exp = Expression.Parse("a = b");
			Assert.IsTrue(exp[0].Equals(VariableName.Resolve("a")));
			Assert.IsTrue(exp[1].Equals(VariableName.Resolve("b")));
			Assert.IsTrue(Equals(exp[2], Operator.Get("=")));

			exp = Expression.Parse("a=Table.Column");
			Assert.AreEqual(exp[0], VariableName.Resolve("a"));
			Assert.AreEqual(exp[1], VariableName.Resolve(TableName.Resolve("Table"), "Column"));
			Assert.IsTrue(Equals(exp[2], Operator.Get("=")));

			exp = Expression.Parse("b > 35");
			Assert.AreEqual(exp[0], VariableName.Resolve("b"));
			Assert.IsTrue(((TObject)exp[1]).ValuesEqual(TObject.CreateInt4(35)));

			exp = Expression.Parse("47");
			Assert.IsTrue(exp.IsConstant);
			Assert.IsTrue(((TObject)exp[0]).ValuesEqual(TObject.CreateInt4(47)));

			exp = Expression.Parse("a * (b + c)");
			Assert.AreEqual(exp[0], VariableName.Resolve("a"));
			Assert.AreEqual(exp[1], VariableName.Resolve("b"));
			Assert.AreEqual(exp[2], VariableName.Resolve("c"));
			Assert.AreEqual(exp[3], Operator.Get("+"));
			Assert.AreEqual(exp[4], Operator.Get("*"));

			exp = Expression.Parse("34 + 159");
			Assert.IsTrue(((TObject)exp[0]).ValuesEqual(TObject.CreateInt4(34)));
			Assert.IsTrue(((TObject)exp[1]).ValuesEqual(TObject.CreateInt4(159)));
			Assert.AreEqual(exp[2], Operator.Get("+"));
			TObject result = exp.Evaluate(null, null, null);
			Assert.IsTrue(exp.IsConstant);
			Assert.IsTrue(result.ValuesEqual(TObject.CreateInt4(193)));
		}

		[Test]
		public void FunctionExpression() {
			Expression exp = Expression.Parse("CHAR_LENGTH(a)");
			Assert.IsTrue(exp[0] is RoutineInvoke);
			RoutineInvoke fdef = (RoutineInvoke) exp[0];
			Assert.AreEqual(fdef.Name, "CHAR_LENGTH");
			Assert.AreEqual(fdef.Arguments.Length, 1);
			Assert.AreEqual(fdef.Arguments[0].AllVariables.Count, 1);
			Assert.AreEqual(fdef.Arguments[0].AllVariables[0], VariableName.Resolve("a"));

			exp = Expression.Parse("CHAR_LENGTH('test')");
			fdef = (RoutineInvoke)exp[0];
			Assert.AreEqual(fdef.Name, "CHAR_LENGTH");
			Assert.AreEqual(fdef.Arguments.Length, 1);
			Assert.AreEqual(fdef.Arguments[0].AllElements.Count, 1);
			TObject result = ((IFunction)fdef.GetFunction(null)).Execute(fdef, null, null, null);
			Assert.IsTrue(result == (TObject) 4);

			exp = Expression.Parse("CHAR_LENGTH(CAST(CURRENT_TIMESTAMP AS VARCHAR))");
			fdef = (RoutineInvoke) exp[0];
			result = ((IFunction) fdef.GetFunction(null)).Execute(fdef, null, null, null);
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

		[Test]
		public void StaticEvaluate() {
			TObject result = Expression.Evaluate("CURRENT_TIMESTAMP");
			Assert.IsTrue(result.TType is TDateType);
			Console.Out.WriteLine("CURRENT_TIMESTAMP = {0}", result);

			IDictionary<string, object> args = new Dictionary<string, object>();
			args["arg0"] = "test_string";
			result = Expression.Evaluate("CHAR_LENGTH(:arg0)", args);
			Assert.IsTrue(result.TType is TNumericType);
			Assert.AreEqual(11, result);
			Console.Out.WriteLine("CHAR_LENGTH(:arg0 = 'test_string') = {0}", result);

			args = new Dictionary<string, object>();
			args["a"] = 12;
			args["b"] = 45;
			args["c"] = 23.65;
			result = Expression.Evaluate("a * (b + c)", args);
			Assert.IsNotNull(result);
			Assert.IsTrue(((BigNumber)result).CompareTo((BigNumber)823.8) == 0);
		}
	}
}