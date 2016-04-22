using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Routines {
	[TestFixture]
	public sealed class PlSqlFunctionTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			CreateFunction(query);
			return true;
		}

		private void CreateFunction(IQuery query) {
			var functionName = ObjectName.Parse("APP.func1");
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new ReturnStatement(SqlExpression.Constant(200)));
			var functionInfo = new PlSqlFunctionInfo(functionName, new RoutineParameter[0], PrimitiveTypes.Integer(), body);
			query.Access().CreateRoutine(functionInfo);
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			var functionName = ObjectName.Parse("APP.func1");
			Query.Access().DropObject(DbObjectType.Routine, functionName);
			return true;
		}

		[Test]
		public void InvokeFunction() {
			var result = Query.SelectFunction(ObjectName.Parse("APP.func1"));

			Assert.IsNotNull(result);
			Assert.IsFalse(Field.IsNullField(result));
			Assert.IsInstanceOf<NumericType>(result.Type);
			Assert.IsInstanceOf<SqlNumber>(result.Value);
			Assert.AreEqual(200, ((SqlNumber) result.Value).ToInt32());
		}
	}
}
