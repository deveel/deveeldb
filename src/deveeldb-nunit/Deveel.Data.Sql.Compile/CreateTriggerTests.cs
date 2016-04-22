using System;

using NUnit.Framework;

namespace Deveel.Data.Sql.Compile {
	[TestFixture]
	public sealed class CreateTriggerTests : SqlCompileTestBase {
		[Test]
		public void CallbackTriggerBeforeInsert() {
			const string sql = "CREATE CALLBACK TRIGGER simpleTrigger BEFORE INSERT ON table1";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
		}

		[Test]
		public void BeforeDeleteOrUpateWithBody() {
			const string sql = @"CREATE OR REPLACE TRIGGER trigger1 BEFORE DELETE OR UPDATE ON table1 FOR EACH ROW
                                   BEGIN
                                      INSERT INTO table2 (oldId, name, new_name) VALUES (OLD.id, OLD.name, NEW.name)
                                   END";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
		}

		[Test]
		public void AfterUpdateCallProcedure() {
			const string sql =
				"CREATE OR REPLACE TRIGGER trigger1 AFTER UPDATE ON table1 FOR EACH ROW CALL updateRegistry(OLD.name, NEW.name)";

			var result = Compile(sql);

			Assert.IsNotNull(result);
			Assert.IsFalse(result.HasErrors);
		}
	}
}
