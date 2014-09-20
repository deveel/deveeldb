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
using System.Collections.Generic;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// A journal for handling namespace clashes between transactions.
	/// </summary>
	/// <remarks>
	/// For example, we would need to generate a conflict if two concurrent
	/// transactions were to drop the same table, or if a procedure and a
	/// table with the same name were generated in concurrent transactions.
	/// </remarks>
	internal sealed class NameSpaceJournal {
		/// <summary>
		/// The commit_id of this journal entry.
		/// </summary>
		public readonly long CommitId;

		/// <summary>
		/// The list of names created in this journal.
		/// </summary>
		public readonly IEnumerable<TableName> CreatedNames;

		/// <summary>
		/// The list of names dropped in this journal.
		/// </summary>
		public readonly IEnumerable<TableName> DroppedNames;

		public NameSpaceJournal(long commitId, IEnumerable<TableName> createdNames, IEnumerable<TableName> droppedNames) {
			CommitId = commitId;
			CreatedNames = createdNames;
			DroppedNames = droppedNames;
		}
	}
}