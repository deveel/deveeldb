// 
//  Copyright 2010-2016 Deveel
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
//


using System;

namespace Deveel.Data.Diagnostics {
	public static class KnownEventMetadata {
		public const string AffectedRows = "session.lastCommand.affectedRows";
		public const string UserName = "session.userName";
		public const string SessionStartTime = "session.startTime";
		public const string LastCommandTime = "session.lastCommandTime";
		public const string LastCommand = "session.lastCommand";
		public const string CommitId = "transaction.commitId";
		public const string CurrentSchema = "transaction.currentSchema";
		public const string IsolationLevel = "transaction.isolationLevel";
		public const string IgnoreIdentifiersCase = "transaction.ignoreIdCase";
		public const string ReadOnlyTransaction = "transaction.readOnly";
		public const string DatabaseName = "database.name";
		public const string SessionCount = "database.sessionCount";
		public const string TableId = "table.id";
		public const string TableName = "table.name";
	}
}
