using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class ConditionalTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			Query.Access().CreateObject(new VariableInfo("a", PrimitiveTypes.Integer(), false) {
				DefaultExpression = SqlExpression.Constant(34)
			});

			base.OnAfterSetup(testName);
		}

		protected override void OnBeforeTearDown(string testName) {
			Query.Access().DropObject(DbObjectType.Variable, new ObjectName("a"));
			base.OnBeforeTearDown(testName);
		}

		[Test]
		public void SimpleConditional() {
			var condition = SqlExpression.Equal(SqlExpression.VariableReference("a"), SqlExpression.Constant(34));
			var ifTrue = new SqlStatement[] {
				new DeclareVariableStatement("b", PrimitiveTypes.String()),
				new AssignVariableStatement(SqlExpression.VariableReference("b"), SqlExpression.Constant(21)),  
			};

			var result = Query.If(condition, ifTrue);

			Assert.IsNotNull(result);
		}
	}
}
