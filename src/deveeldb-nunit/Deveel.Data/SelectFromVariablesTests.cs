using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectFromVariablesTests : ContextBasedTest {
		protected override void OnAfterSetup(string testName) {
			if (testName == "SelectSingleVariable") {
				AdminQuery.Access().CreateObject(new VariableInfo("a", PrimitiveTypes.Bit(), false));
			}

			base.OnAfterSetup(testName);
		}

		[Test]
		public void SelectCurrentVariables() {
			const string sql = "SELECT * FROM system.vars";

			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			var result = AdminQuery.Select(query);

			Assert.IsNotNull(result);

			Row row = null;
			Assert.DoesNotThrow(() => row = result.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);
		}

		[Test]
		public void SelectSingleVariable() {
			const string sql = "SELECT * FROM system.vars WHERE var = 'a'";

			var query = (SqlQueryExpression)SqlExpression.Parse(sql);

			var result = AdminQuery.Select(query);

			Assert.IsNotNull(result);

			Row row = null;
			Assert.DoesNotThrow(() => row = result.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);

			var name = row.GetValue("var");
			var type = row.GetValue("type");
			var value = row.GetValue("value");
			var constant = row.GetValue("constant");

			Assert.IsFalse(Field.IsNullField(name));
			Assert.IsFalse(Field.IsNullField(type));
			Assert.IsTrue(value.IsNull);
			Assert.IsFalse(Field.IsNullField(constant));
		}
	}
}
