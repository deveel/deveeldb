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
		public void CallbackTriggerOnBeforeAllEvents() {
			Assert.IsTrue(Connection.State == ConnectionState.Open);

			TriggerEventType eventType = 0;
			int count = 0;

			DeveelDbTrigger trigger = new DeveelDbTrigger(Connection, "BeforePersonModified", TriggerEventType.AllBefore);
			trigger.ObjectName = "Person";
			trigger.Create();
			trigger.Subscribe(invoke => {
				Console.Out.WriteLine("Trigger {0} was fired on {1} (fired {2} times for {3})", invoke.TriggerName, invoke.ObjectName, invoke.Count, invoke.EventType);

				Assert.AreEqual(count, invoke.Count);
				Assert.AreEqual(eventType, invoke.EventType);
				Assert.AreEqual("PersonModified", invoke.TriggerName);
			});

			using (var transaction = Connection.BeginTransaction()) {
				eventType = TriggerEventType.Insert;

				Console.Out.WriteLine("Inserting a new entry in the table 'Person'.");
				count = ExecuteNonQuery("INSERT INTO Person (name, age, lives_in) VALUES ('Lorenzo Thione', 30, 'Texas')");

				Assert.AreEqual(1, count);

				transaction.Commit();
			}

			using (var transaction = Connection.BeginTransaction()) {
				// wait few milliseconds to be sure the test will succeed...
				Thread.Sleep(300);

				eventType = TriggerEventType.Update;
				count = ExecuteNonQuery("UPDATE Person SET lives_in = 'San Francisco' WHERE name = 'Lorenzo Thione'");

				Assert.AreEqual(1, count);

				transaction.Commit();
			}

			Thread.Sleep(300);

			using (var transaction = Connection.BeginTransaction()) {
				count = ExecuteNonQuery("DELETE FROM Person WHERE name = 'Lorenzo Thione'");

				Assert.AreEqual(1, count);

				transaction.Commit();
			}
		}

		[Test]
		public void CallbackTriggerOnInsert() {
			DeveelDbConnection connection = Connection;
			Assert.IsTrue(connection.State == ConnectionState.Open);

			DeveelDbTrigger trigger = new DeveelDbTrigger(connection, "PersonCreated", TriggerEventType.BeforeInsert);
			trigger.Subscribe(PersonTableModified);

			Console.Out.WriteLine("Inserting a new entry in the table 'Person'.");
			DeveelDbCommand command = connection.CreateCommand("INSERT INTO Person (name, age, lives_in) VALUES ('Lorenzo Thione', 30, 'Texas')");
			int count = command.ExecuteNonQuery();

			Assert.AreEqual(1, count);

			trigger.Dispose();
		}

		private static void PersonTableModified(TriggerInvoke invoke) {
			Console.Out.WriteLine("Trigger {0} was fired on {1} (fired {2} times for {3})", invoke.TriggerName, invoke.ObjectName, invoke.Count, invoke.EventType);
		}
	}
}