using System;
using System.Linq;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class SelectProductInfoTests : ContextBasedTest {
		[Test]
		public void SelectSimple() {
			const string sql = "SELECT * FROM SYSTEM.product_info";

			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			var result = Query.Select(query);

			Assert.IsNotNull(result);

			Row row = null;
			Assert.DoesNotThrow(() => row = result.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);

			var varName = row.GetValue("var");
			var varValue = row.GetValue("value");

			Assert.AreEqual("title", varName.Value.ToString());
			Assert.AreEqual("DeveelDB", varValue.Value.ToString());
		}
	}
}
