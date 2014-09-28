using System;
using System.Data;
using System.Threading;

using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Routines;
using Deveel.Data.Sql;

using NUnit.Framework;

namespace Deveel.Data.Client {
	[TestFixture]
	public sealed class TriggerTest : SqlTestBase {
		[Test]
		public void CallbackTriggerOnAllEvents() {
			Assert.IsTrue(Connection.State == ConnectionState.Open);
			
			DeveelDbTrigger trigger = new DeveelDbTrigger(Connection, "PersonCreated");
			trigger.Subscribe(PersonTableModified);

			Console.Out.WriteLine("Inserting a new entry in the table 'Person'.");
			int count = ExecuteNonQuery("INSERT INTO Person (name, age, lives_in) VALUES ('Lorenzo Thione', 30, 'Texas')");
			
			Assert.AreEqual(1, count);
			
			// wait few milliseconds to be sure the test will succeed...
			Thread.Sleep(300);

			count = ExecuteNonQuery("UPDATE Person SET lives_in = 'San Francisco' WHERE name = 'Lorenzo Thione'");

			Assert.AreEqual(1, count);

			Thread.Sleep(300);

			count = ExecuteNonQuery("DELETE FROM Person WHERE name = 'Lorenzo Thione'");

			Assert.AreEqual(1, count);
		}

		[Test]
		public void CallbackTriggerOnInsert() {
			DeveelDbConnection connection = Connection;
			Assert.IsTrue(connection.State == ConnectionState.Open);

			DeveelDbTrigger trigger = new DeveelDbTrigger(connection, "PersonCreated");
			trigger.Subscribe(PersonTableModified);

			Console.Out.WriteLine("Inserting a new entry in the table 'Person'.");
			DeveelDbCommand command = connection.CreateCommand("INSERT INTO Person (name, age, lives_in) VALUES ('Lorenzo Thione', 30, 'Texas')");
			int count = command.ExecuteNonQuery();

			Assert.AreEqual(1, count);

			trigger.Dispose();
		}

		private static void PersonTableModified(TriggerInvoke invoke) {
			Console.Out.WriteLine("Trigger {0} was fired on {1} (fired {2} times)", invoke.TriggerName, invoke.ObjectName, invoke.Count);

			if (invoke.IsBefore && invoke.IsInsert) {
				Console.Out.WriteLine("Adding an entry to the table 'Person'.");
			} else if (invoke.IsAfter && invoke.IsInsert) {
				Console.Out.WriteLine("Added an entry to the table 'Person'.");
			}
		}
	}
}