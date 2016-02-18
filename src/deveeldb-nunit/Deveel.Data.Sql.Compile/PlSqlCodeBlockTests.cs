using System;
using System.Linq;

using Deveel.Data.Sql.Statements;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class PlSqlCodeBlockTests : SqlCompileTestBase {
		[Test]
		public void EmptyBlock() {
			const string sql = @"BEGIN END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);

			Assert.AreEqual(1, result.CodeObjects.Count);

			var obj = result.CodeObjects.ElementAt(0);

			Assert.IsNotNull(obj);
			Assert.IsInstanceOf<PlSqlBlock>(obj);
		}
	}
}
