using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	public class FunctionsTest : SqlTestBase {
		[Test]
		public void CreateFunctionReturnsType() {
			string sql = "CREATE FUNCTION test (a INTEGER, b STRING) "+
				"RETURNS INTEGER "+
				"LANGUAGE CSHARP NAME '" + typeof(FunctionsTestClass).AssemblyQualifiedName + "'";

			Assert.DoesNotThrow(() => ExecuteNonQuery(sql));
		}
	}

	public static class FunctionsTestClass {
		public static int Invoke(int a, string b) {
			return 223;
		}
	}
}
