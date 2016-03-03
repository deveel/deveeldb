using System;

using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Sql.Variables {
	[TestFixture]
	public class VariableManagerTests {
		private VariableManager variableManager;

		[SetUp]
		public void SetUp() {
			variableManager = new VariableManager(null);

			if (TestContext.CurrentContext.Test.Name != "DefineVariable") {
				var varInfo = new VariableInfo("a", PrimitiveTypes.String(), false);
				variableManager.DefineVariable(varInfo);
			}
		}

		[TearDown]
		public void TearDown() {
			variableManager.Dispose();
			variableManager = null;
		}

		[Test]
		public void DefineVariable() {
			var varInfo = new VariableInfo("a", PrimitiveTypes.String(), false);
			variableManager.DefineVariable(varInfo);
		}

		[TestCase("a", true)]
		[TestCase("b", false)]
		public void DropVariable(string varName, bool expectedResult) {
			var result = variableManager.DropVariable(varName);
			Assert.AreEqual(expectedResult, result);
		}

		[Test]
		public void GetVariable() {
			var variable = variableManager.GetVariable("a");

			Assert.IsNotNull(variable);
			Assert.IsInstanceOf<StringType>(variable.Type);
			Assert.IsNull(variable.Value);
			Assert.IsFalse(variable.IsConstant);
			Assert.IsFalse(variable.IsNotNull);
		}
	}
}
