using System;

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SelectPrivsTests : ContextBasedTest {
		[Test]
		public void SelectAllPrivs() {
			const string sql = "SELECT * FROM system.privs";

			var query = (SqlQueryExpression) SqlExpression.Parse(sql);

			var cursor = Query.Select(query);

			Assert.IsNotNull(cursor);

			Row row = null;
			Assert.DoesNotThrow(() => row = cursor.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);

			var privBit = row.GetValue("priv_bit");
			var desc = row.GetValue("description");

			Assert.AreEqual(0, ((SqlNumber) privBit.Value).ToInt32());
			Assert.AreEqual("NONE", desc.Value.ToString());
		}

		[TestCase("SELECT")]
		[TestCase("UPDATE")]
		[TestCase("DELETE")]
		public void SelectOnePriv(string name) {
			var sql = String.Format("SELECT * FROM system.privs WHERE description = '{0}'", name);

			var query = (SqlQueryExpression)SqlExpression.Parse(sql);

			var cursor = Query.Select(query);

			Assert.IsNotNull(cursor);

			Row row = null;
			Assert.DoesNotThrow(() => row = cursor.Fetch(FetchDirection.Next, -1));
			Assert.IsNotNull(row);

			var privBit = row.GetValue("priv_bit");
			var desc = row.GetValue("description");

			var privs = (Privileges) Enum.Parse(typeof(Privileges), name, true);

			Assert.AreEqual((int)privs, ((SqlNumber)privBit.Value).ToInt32());
			Assert.AreEqual(name.ToUpperInvariant(), desc.Value.ToString().ToUpperInvariant());
		}
	}
}
