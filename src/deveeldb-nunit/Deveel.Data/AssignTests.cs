using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class AssignTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			Query.Access().CreateObject(new VariableInfo("a", PrimitiveTypes.Integer(), false));
			Query.Access().CreateObject(new VariableInfo("b", PrimitiveTypes.Integer(), true) {
				DefaultExpression = SqlExpression.Constant(56)
			});
		}

		[Test]
		public void Existing() {
			var result = Query.Assign("a", SqlExpression.Constant(7689));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);

			var variable = Query.Context.FindVariable("a");
			Assert.IsNotNull(variable);
			Assert.IsNotNull(variable.Expression);
			Assert.IsNotNull(variable.Value);

			var value = ((SqlNumber) variable.Value.Value).ToDouble();
			Assert.AreEqual(7689, value);
		}

		[Test]
		public void NotExisting() {
			var result = Query.Assign("c", SqlExpression.Constant(7689));

			Assert.IsNotNull(result);
			Assert.IsInstanceOf<NumericType>(result.Type);

			var variable = Query.Context.FindVariable("c");
			Assert.IsNotNull(variable);
			Assert.IsNotNull(variable.Expression);
			Assert.IsNotNull(variable.Value);

			var value = ((SqlNumber)variable.Value.Value).ToDouble();
			Assert.AreEqual(7689, value);
		}

		[Test]
		public void ToConstant() {
			Assert.Throws<StatementException>(() => Query.Assign("b", SqlExpression.Constant(453)));
		}
	}
}
