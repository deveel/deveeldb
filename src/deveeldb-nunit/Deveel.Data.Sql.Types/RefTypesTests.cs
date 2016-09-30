using System;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data.Types {
	[TestFixture]
	public sealed class RefTypesTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.test_table"));
			tableInfo.AddColumn("a", PrimitiveTypes.Integer());

			query.Access().CreateObject(tableInfo);
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			query.Access().DropObject(DbObjectType.Table, ObjectName.Parse("APP.test_table"));
			return true;
		}

		[TestCase("APP.test_table.a", "INTEGER")]
		public void ColumnType(string column, string typeString) {
			var refType = new FieldRefType(ObjectName.Parse(column));
			var resolved = refType.Resolve(AdminQuery);

			Assert.IsNotNull(resolved);
			Assert.AreEqual(resolved.ToString(), typeString);
		}

		[TestCase("APP.test_table", "")]
		public void RowType(string tableName, string typeString) {
			var refType = new RowRefType(ObjectName.Parse(tableName));
			SqlType resolved;
			Assert.Throws<NotImplementedException>(() => resolved = refType.Resolve(AdminQuery));
		}
	}
}
