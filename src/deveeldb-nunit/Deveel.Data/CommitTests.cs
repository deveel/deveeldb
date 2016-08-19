using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class CommitTests : ContextBasedTest {
		private IQuery TestQuery { get; set; }

		protected override void OnAfterSetup(string testName) {
			var session = CreateAdminSession(Database);
			TestQuery = CreateQuery(session);
		}

		protected override bool OnSetUp(string testName, IQuery query) {
			if (testName.EndsWith("Violation")) {
				CreateTables(query, testName);

				if (testName.Equals("UniqueKeyViolation") ||
					testName.Equals("PrimaryKeyViolation")) {
					InsertData(query);
				} else if (testName.Equals("CheckViolation")) {
					query.AddCheck(new ObjectName("a"),
						SqlExpression.SmallerOrEqualThan(SqlExpression.Reference(new ObjectName("id")), SqlExpression.Constant(100)));
				}
			}

			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			if (testName.EndsWith("Violation"))
				DropTables(query);

			return true;
		}

		private void CreateTables(IQuery query, string testsName) {
			var columns = new List<SqlTableColumn> {
				new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("name", PrimitiveTypes.String())
			};

			if (testsName.Equals("NotNullColumnViolation"))
				columns.Add(new SqlTableColumn("age", PrimitiveTypes.Integer()) {
					IsNotNull = true
				});

			query.CreateTable(new ObjectName("a"), columns.ToArray());
			query.AddPrimaryKey(new ObjectName("a"), new []{"id"});
			query.AddUniqueKey(new ObjectName("a"), new[] {"name"});

			query.CreateTable(new ObjectName("b"), new SqlTableColumn("id", PrimitiveTypes.Integer()),
				new SqlTableColumn("other_id", PrimitiveTypes.Integer()));
			query.AddPrimaryKey(new ObjectName("b"), new[] {"id"});
			query.AddForeignKey(new ObjectName("b"), new[] {"other_id"}, new ObjectName("a"), new[] {"id"},
				ForeignKeyAction.Cascade, ForeignKeyAction.NoAction);
		}

		private void InsertData(IQuery query) {
			query.Insert(new ObjectName("a"),
				new SqlExpression[] {SqlExpression.Constant(1), SqlExpression.Constant("Antonello Provenzano")});
		}

		private void DropTables(IQuery query) {
			query.DropTable(new ObjectName("b"), true);
			query.DropTable(new ObjectName("a"), true);
		}

		[Test]
		public void EmptyCommit() {
			TestQuery.Commit();
		}

		[Test]
		public void NormalCommit() {
			TestQuery.CreateTable(new ObjectName("test"), new SqlTableColumn("a", PrimitiveTypes.Integer()));
			TestQuery.Commit();
		}

		[Test]
		public void CommitChangesAndDropTable() {
			TestQuery.CreateTable(new ObjectName("test"), new SqlTableColumn("a", PrimitiveTypes.Integer()));
			TestQuery.Insert(new ObjectName("test"), new SqlExpression[] {SqlExpression.Constant(2)});
			TestQuery.DropTable(new ObjectName("test"));
			TestQuery.Commit();
		}

		[Test]
		public void ForeignKeyViolation() {
			TestQuery.Insert(new ObjectName("b"), new SqlExpression[] {SqlExpression.Constant(1), SqlExpression.Constant(2)});

			var expected = Is.InstanceOf<ConstraintViolationException>()
				.And.TypeOf(typeof(ForeignKeyViolationException))
				.And.Property("TableName").EqualTo(ObjectName.Parse("APP.b"))
				.And.Property("ColumnNames").EqualTo(new[] {"other_id"})
				.And.Property("LinkedTableName").EqualTo(ObjectName.Parse("APP.a"))
				.And.Property("LinkedColumnNames").EqualTo(new[] {"id"});

			Assert.Throws(expected, () => TestQuery.Commit());
		}

		[Test]
		public void UniqueKeyViolation() {
			TestQuery.Insert(new ObjectName("a"),
				new SqlExpression[] {SqlExpression.Constant(2), SqlExpression.Constant("Antonello Provenzano")});

			var expected = Is.InstanceOf<ConstraintViolationException>()
				.And.TypeOf<UniqueKeyViolationException>()
				.And.Property("TableName").EqualTo(ObjectName.Parse("APP.a"))
				.And.Property("ColumnNames").EqualTo(new [] {"name"});

			Assert.Throws(expected, () => TestQuery.Commit());
		}

		[Test]
		public void PrimaryKeyViolation() {
			TestQuery.Insert(new ObjectName("a"),
				new SqlExpression[] {
					SqlExpression.Constant(1),
					SqlExpression.Constant("Sebastiano Provenzano")
				});

			var expected = Is.InstanceOf<ConstraintViolationException>()
				.And.TypeOf<PrimaryKeyViolationException>()
				.And.Property("TableName").EqualTo(ObjectName.Parse("APP.a"))
				.And.Property("ColumnNames").EqualTo(new[] {"id"});

			Assert.Throws(expected, () => TestQuery.Commit());
		}

		[Test]
		public void CheckViolation() {
			TestQuery.Insert(new ObjectName("a"), new SqlExpression[] {
				SqlExpression.Constant(101),
				SqlExpression.Constant("Sebastiano Provenzano"),
			});

			var expected = Is.InstanceOf<ConstraintViolationException>()
				.And.TypeOf<CheckViolationException>()
				.And.Property("TableName").EqualTo(ObjectName.Parse("APP.a"));

			Assert.Throws(expected, () => TestQuery.Commit());
		}
	}
}
