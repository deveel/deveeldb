using System;

using Deveel.Data.Routines;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class DropFunctionTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var funcName = ObjectName.Parse("APP.func1");
			var returnType = PrimitiveTypes.String();
			var body = new PlSqlBlockStatement();
			body.Statements.Add(new ReturnStatement(SqlExpression.Constant("Hello!")));

			var funtionInfo = new PlSqlFunctionInfo(funcName, new RoutineParameter[0], returnType, body);
			query.Access().CreateObject(funtionInfo);

			return true;
		}

		[Test]
		public void Existing() {
			var funcName = ObjectName.Parse("APP.func1");

			Query.DropFunction(funcName);

			var exists = Query.Access().ObjectExists(DbObjectType.Routine, funcName);
			Assert.IsFalse(exists);
		}

		[Test]
		public void NotExisting() {
			var funcName = ObjectName.Parse("APP.func2");

			Assert.Throws<ObjectNotFoundException>(() => Query.DropFunction(funcName));
		}

		[Test]
		public void NotExisting_IfExistsClause() {
			var funcName = ObjectName.Parse("APP.func2");

			Assert.DoesNotThrow(() => Query.DropFunction(funcName, true));
		}
	}
}
