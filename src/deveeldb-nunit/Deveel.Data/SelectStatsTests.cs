using System;

using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectStatsTests : ContextBasedTest {
		[Test]
		public void AllStats() {
			const string sql = "SELECT * FROM system.stats";

			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			var result = AdminQuery.Select(query);

			Assert.IsNotNull(result);

			Row row = null;
			Assert.DoesNotThrow(() => row = result.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);
		}
	}
}
