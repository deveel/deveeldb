using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Tables {
	[TestFixture]
	public static class RowIdTests {
		[Test]
		public static void NotNull() {
			var nullRowId = RowId.Null;
			var rowId = new RowId(1, 32);

			Assert.AreNotEqual(rowId, nullRowId);
		}

		[TestCase("#V-30", -1, 30)]
		[TestCase("3-89199", 3, 89199)]
		public static void Parse(string s, int tableId, long rowNumber) {
			RowId rowId = RowId.Null;
			Assert.DoesNotThrow(() => rowId = RowId.Parse(s));

			Assert.IsFalse(rowId.IsNull);
			Assert.AreEqual(tableId, rowId.TableId);
			Assert.AreEqual(rowNumber, rowId.RowNumber);
		}

		[TestCase(3, 2019, "3-2019")]
		[TestCase(-1, 2278, "#V-2278")]
		public static void ToString(int tableId, long rowNumber, string expected) {
			var rowId = new RowId(tableId, (int) rowNumber);
			var s = rowId.ToString();

			Assert.AreEqual(expected, s);
		}
	}
}
