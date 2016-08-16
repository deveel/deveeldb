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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Sequences;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public sealed class SequenceManagerTests : ContextBasedTest {
		private ObjectName testSequenceName = ObjectName.Parse("APP.test_sequence");

		protected override IQuery CreateQuery(ISession session) {
			var query = base.CreateQuery(session);

			if (TestContext.CurrentContext.Test.Name != "CreateNormalSequence") {
				var seqInfo = new SequenceInfo(testSequenceName, new SqlNumber(0), new SqlNumber(1), new SqlNumber(0), new SqlNumber(Int64.MaxValue), 126);
				query.Access().CreateObject(seqInfo);
			}

			return query;
		}

		[Test]
		public void IncremementSequenceValue() {
			var sequenceManager = new SequenceManager(AdminSession.Transaction);

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
