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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Cursors;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CloseStatement : SqlStatement, IPlSqlStatement {
		public CloseStatement(string cursorName) {
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
		}

		private CloseStatement(SerializationInfo info, StreamingContext context) {
			CursorName = info.GetString("CursorName");
		}

		public string CursorName { get; private set; }

		protected override void ExecuteStatement(ExecutionContext context) {
			var cursor = context.Request.Context.FindCursor(CursorName);
			if (cursor == null)
				throw new ObjectNotFoundException(new ObjectName(CursorName));

			cursor.Close();
		}

		protected override void GetData(SerializationInfo info) {
			info.AddValue("CursorName", CursorName);
		}

	}
}
