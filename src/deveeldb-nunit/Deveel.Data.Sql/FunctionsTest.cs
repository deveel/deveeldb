using System;

using NUnit.Framework;

namespace Deveel.Data.Sql {
	public class FunctionsTest : SqlTestBase {
		[Test]
		public void CreateFunctionReturnsType() {
			string sql = "CREATE FUNCTION testAdd (a INTEGER, b STRING) "+
				"RETURNS INTEGER "+
				"LANGUAGE CSHARP NAME '" + typeof(FunctionsTestClass).AssemblyQualifiedName + "'";

			Assert.DoesNotThrow(() => ExecuteNonQuery(sql));
		}

		[Test]
		public void CallFunction() {
			Assert.Inconclusive("Not implemented");
		}
	}

	public static class FunctionsTestClass {
		public static int Invoke(int a, string b) {
			int c;
			if (String.IsNullOrEmpty(b) ||
				!Int32.TryParse(b, out c))
				return a;

			return a + c;
		}
	}
}
