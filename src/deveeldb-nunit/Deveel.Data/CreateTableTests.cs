using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture(StorageType.InMemory)]
	[TestFixture(StorageType.SingleFile)]
	[TestFixture(Data.StorageType.JournaledFile)]
	public sealed class CreateTableTests : ContextBasedTest {
		public CreateTableTests(StorageType storageType)
			: base(storageType) {
		}

		[Test]
		public void SimpleCreate() {
			var tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()),
			};

			Query.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}

		[Test]
		public void WithColumnDefault() {
			var tableName = ObjectName.Parse("APP.test");
			var columns = new SqlTableColumn[] {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.VarChar()) {
					DefaultExpression = SqlExpression.Parse("((67 * 90) + 22)")
				},
				new SqlTableColumn("date", PrimitiveTypes.TimeStamp()) {
					DefaultExpression = SqlExpression.Parse("GetDate()")
				}
			};

			Query.CreateTable(tableName, columns);

			// TODO: Assert it exists and has the structure desired...
		}
	}
}
