using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	[TestFixture]
	public class VariableTests : ContextBasedTest {
		protected override void OnSetUp(string testName) {
			if (testName != "DeclareVariable")
				QueryContext.DeclareVariable("a", PrimitiveTypes.String());

			base.OnSetUp(testName);
		}

		[Test]
		public void DeclareVariable() {
			QueryContext.DeclareVariable("a", PrimitiveTypes.String());
		}

		[Test]
		public void GetVariable() {
			var variable = QueryContext.FindVariable("a");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<StringType>(variable.Type);
		}

		[Test]
		public void SetExistingVariable() {
			QueryContext.SetVariable("a", SqlExpression.Constant("test"));

			var variable = QueryContext.FindVariable("a");
			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<StringType>(variable.Type);
			Assert.IsNotNull(variable.Value);
			Assert.IsInstanceOf<StringType>(variable.Value.Type);
		}

		[Test]
		public void SetNotExistingVariable() {
			QueryContext.SetVariable("b", SqlExpression.Constant(23));

			var variable = QueryContext.FindVariable("b");
			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<NumericType>(variable.Type);
			Assert.IsNotNull(variable.Value);
			Assert.IsInstanceOf<NumericType>(variable.Value.Type);
		}
	}
}
