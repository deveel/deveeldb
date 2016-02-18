using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class DropTypeTests : SqlCompileTestBase {
		[Test]
		public void DropType() {
			const string sql = "DROP TYPE test_type";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var statement = result.CodeObjects.ElementAt(0);

			Assert.IsInstanceOf<DropTypeStatement>(statement);

			var dropType = (DropTypeStatement) statement;
			var typeName = new ObjectName("test_type");

			Assert.AreEqual(typeName, dropType.TypeName);
		}
	}
}