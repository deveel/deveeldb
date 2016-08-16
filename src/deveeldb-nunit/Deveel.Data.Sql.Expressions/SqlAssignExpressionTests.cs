using System;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using Moq;

using NUnit.Framework;

namespace Deveel.Data.Sql.Expressions {
	public class SqlAssignExpressionTests {
		private IVariableResolver variableResolver;

		[SetUp]
		public void SetUp() {
			var variableInfo = new VariableInfo("t", PrimitiveTypes.String(), false);
			var vrMock = new Mock<IVariableResolver>();
			vrMock.Setup(x => x.Resolve(It.Is<ObjectName>(name => name.FullName.Equals("t"))))
				.Returns<ObjectName>(name => new Variable(variableInfo));
			variableResolver = vrMock.Object;
		}

		[Test]
		public void SimpleAssign() {
			var assign = SqlExpression.Assign(SqlExpression.VariableReference("t"), SqlExpression.Constant("test"));
			var result = assign.EvaluateToConstant(null, variableResolver);

			Assert.IsFalse(Field.IsNullField(result));
			Assert.IsInstanceOf<StringType>(result.Type);

			var value = ((SqlString) result.Value).ToString();

			Assert.AreEqual("test", value);
		}

		[Test]
		public void StringFormat() {
			var assign = SqlExpression.Assign(SqlExpression.VariableReference("t"), SqlExpression.Constant("test"));

			var expected = ":t := 'test'";
			Assert.AreEqual(expected, assign.ToString());
		}
	}
}
