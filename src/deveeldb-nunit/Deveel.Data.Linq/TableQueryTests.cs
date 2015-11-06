using System;
using System.Linq;

using Deveel.Data.Mapping;
using Deveel.Data.Sql;
using Deveel.Data.Types;

using NUnit.Framework;

namespace Deveel.Data.Linq {
	[TestFixture]
	public class TableQueryTests {
		private ITable table;

		[SetUp]
		public void TestSetup() {
			var tableInfo = new TableInfo(ObjectName.Parse("APP.people"));
			tableInfo.AddColumn("id", PrimitiveTypes.Bit());
			tableInfo.AddColumn("first_name", PrimitiveTypes.String(), true);
			tableInfo.AddColumn("last_name", PrimitiveTypes.String());
			tableInfo.AddColumn("age", PrimitiveTypes.TinyInt());
			table = new TemporaryTable(tableInfo);

			var tempTable = (TemporaryTable) table;
			tempTable.NewRow(new[] {
				DataObject.BigInt(1), 
				DataObject.String("Antonello"), 
				DataObject.String("Provenzano"),
				DataObject.Null()
			});
			tempTable.NewRow(new[] {
				DataObject.BigInt(2), 
				DataObject.String("Moritz"), 
				DataObject.String("Krull"), 
				DataObject.TinyInt(31)
			});

			tempTable.BuildIndexes();
		}

		[Test]
		public void ProjectFirst() {
			IQueryable<Person> queryable = null;
			Assert.DoesNotThrow(() => queryable = table.AsQueryable<Person>());
			Assert.IsNotNull(queryable);

			Person first = null;
			Assert.DoesNotThrow(() => first = queryable.First());
			Assert.IsNotNull(first);
			Assert.AreEqual("Antonello", first.FirstName);
			Assert.AreEqual("Provenzano", first.LastName);
			Assert.AreEqual(1, first.Id);
			Assert.AreEqual(0, first.Age);
		}

		class Person {
			[Column("id")]
			public int Id;

			[Column("first_name")]
			public string FirstName { get; set; }

			[Column("last_name")]
			public string LastName { get; set; }

			[Column("age")]
			public int Age { get; set; }
		}
	}
}
