using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	public class FunctionsTest : SqlTestBase {
		[Test]
		public void CreateFunctionReturnsType() {
			const string sql = "CREATE FUNCTION test (a INTEGER, b STRING) "+
				"RETURNS INTEGER "+
				"LANGUAGE CSHARP NAME 'Deveel.Data.Sql.TestFunctionClass'";

			Assert.DoesNotThrow(() => ExecuteNonQuery(sql));
		}
	}

	public static class FunctionsTestClass {
		public static int Invoke(int a, string b) {
			return 223;
		}
	}
}
