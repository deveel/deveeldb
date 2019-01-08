using System;

using Xunit;

namespace Deveel.Data.Sql.Indexes
{
	public static class IndexSetInfoTests
	{
		[Fact]
		public static void CreateNewIndexSet() {
			var tableName = ObjectName.Parse("sys.table1");
			var indexSetInfo = new IndexSetInfo(tableName);

			Assert.Equal(tableName, indexSetInfo.TableName);
			Assert.False(indexSetInfo.IsReadOnly);
			Assert.Empty(indexSetInfo.Indexes);
		}

		[Fact]
		public static void AddIndexes() {
			var tableName = ObjectName.Parse("sys.table1");
			var indexSetInfo = new IndexSetInfo(tableName);

			Assert.Equal(tableName, indexSetInfo.TableName);
			Assert.False(indexSetInfo.IsReadOnly);
			Assert.Empty(indexSetInfo.Indexes);

			indexSetInfo.Indexes.Add(new IndexInfo("idx1", tableName, new []{"a", "b"}));
			Assert.NotEmpty(indexSetInfo.Indexes);
			Assert.Equal(0, indexSetInfo.FindIndexForColumns(new []{"a", "b"}));
		}
	}
}
