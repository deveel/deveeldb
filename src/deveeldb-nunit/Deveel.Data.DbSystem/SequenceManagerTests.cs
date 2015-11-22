// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;

using Deveel.Data.Configuration;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Transactions;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SequenceManagerTests {
		private ObjectName testSequenceName;
			
		[SetUp]
		public void TestSetup() {
			testSequenceName = ObjectName.Parse("APP.test_sequence");

			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", "testdb");

			var builder = new SystemBuilder();
			var systemContext = builder.BuildContext();
			var dbContext = new DatabaseContext(systemContext, dbConfig);
			var database = new Database(dbContext);
			database.Create("SA", "12345");
			database.Open();

			transaction = database.CreateTransaction(IsolationLevel.Serializable);

			if (TestContext.CurrentContext.Test.Name != "CreateNormalSequence") {
				var seqInfo = new SequenceInfo(testSequenceName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), 126);
				transaction.CreateSequence(seqInfo);
			}
		}

		private ITransaction transaction;

		[Test]
		public void CreateNormalSequence() {
			var sequenceManager = new SequenceManager(transaction);

			var sequenceName = ObjectName.Parse("APP.test_sequence");
			var seqInfo = new SequenceInfo(sequenceName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), 126);

			ISequence sequence =null;
			Assert.DoesNotThrow(() => sequence = sequenceManager.CreateSequence(seqInfo));
			Assert.IsNotNull(sequence);
		}

		[Test]
		public void CreateNativeSequence() {
			var sequenceManager = new SequenceManager(transaction);

			var tableName = ObjectName.Parse("APP.test_table");
			var seqInfo = SequenceInfo.Native(tableName);

			ISequence sequence = null;
			Assert.DoesNotThrow(() => sequence = sequenceManager.CreateSequence(seqInfo));
			Assert.IsNotNull(sequence);
		}

		[Test]
		public void IncremementSequenceValue() {
			var sequenceManager = new SequenceManager(transaction);

			ISequence sequence = null;
			Assert.DoesNotThrow(() => sequence = sequenceManager.GetSequence(testSequenceName));
			Assert.IsNotNull(sequence);

			SqlNumber currentValue = SqlNumber.Null;
			Assert.DoesNotThrow(() => currentValue = sequence.NextValue());
			Assert.IsNotNull(currentValue);
			Assert.AreEqual(new SqlNumber(1), currentValue);
		}
	}
}
