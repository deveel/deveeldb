using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Serialization {
	public sealed class ExpressionSerializationTests : SerializationTestBase {
		[Test]
		public void Assignment() {
			var assign = SqlExpression.Assign(SqlExpression.Reference(new ObjectName("a")), SqlExpression.Constant(22));
			SerializeAndAssert(assign, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.Assign, deserialized.ExpressionType);
				Assert.IsInstanceOf<SqlReferenceExpression>(deserialized.ReferenceExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.ValueExpression);
			});
		}

		[Test]
		public void Cast() {
			var cast = SqlExpression.Cast(SqlExpression.Constant(45), PrimitiveTypes.String());
			SerializeAndAssert(cast, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.Cast, deserialized.ExpressionType);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.Value);
				Assert.IsInstanceOf<StringType>(deserialized.SqlType);
			});
		}

		[Test]
		public void Conditional() {
			var test = SqlExpression.FunctionCall("test");
			var trueExp = SqlExpression.Constant(45);
			var falseExp = SqlExpression.FunctionCall("testReturn");
			var condition = SqlExpression.Conditional(test, trueExp, falseExp);

			SerializeAndAssert(condition, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.Conditional, deserialized.ExpressionType);
				Assert.IsInstanceOf<SqlFunctionCallExpression>(deserialized.TestExpression);
				Assert.IsInstanceOf<SqlConstantExpression>(deserialized.TrueExpression);
				Assert.IsInstanceOf<SqlFunctionCallExpression>(deserialized.FalseExpression);
			});
		}

		[Test]
		public void SimpleBinary() {
			var eq = SqlExpression.Equal(SqlExpression.Reference(new ObjectName("a")), SqlExpression.Constant(455));
			SerializeAndAssert(eq, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.Equal, deserialized.ExpressionType);
				Assert.IsInstanceOf<SqlReferenceExpression>(eq.Left);
				Assert.IsInstanceOf<SqlConstantExpression>(eq.Right);
			});
		}

		[Test]
		public void Constant() {
			var constant = SqlExpression.Constant(0199.001);
			SerializeAndAssert(constant, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.Constant, constant.ExpressionType);
				Assert.IsFalse(Field.IsNullField(deserialized.Value));
				Assert.IsInstanceOf<NumericType>(deserialized.Value.Type);
			});
		}

		[Test]
		public void FunctionCall_NoArgs() {
			var func = SqlExpression.FunctionCall("test");
			SerializeAndAssert(func, (serialized, deserialized) => {
				Assert.AreEqual(SqlExpressionType.FunctionCall, deserialized.ExpressionType);
				Assert.IsNotNull(deserialized.FunctioName);
				Assert.IsEmpty(deserialized.Arguments);
			});
		}
	}
}
