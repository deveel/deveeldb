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
	public sealed class DropSequenceTests : ContextBasedTest {
		protected override bool OnSetUp(string testName, IQuery query) {
			var seqName = ObjectName.Parse("APP.seq1");
			query.Access().CreateObject(new SequenceInfo(seqName, SqlNumber.Zero, SqlNumber.One, SqlNumber.Zero,
				new SqlNumber(Int64.MaxValue), Int32.MaxValue));
			return true;
		}

		protected override bool OnTearDown(string testName, IQuery query) {
			return true;
		}

		[Test]
		public void Existing() {
			var seqName = ObjectName.Parse("APP.seq1");

			AdminQuery.DropSequence(seqName);

			var exists = AdminQuery.Access().ObjectExists(DbObjectType.Sequence, seqName);
			Assert.IsFalse(exists);
		}
	}
}
