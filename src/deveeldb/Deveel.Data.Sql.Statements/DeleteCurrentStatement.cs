// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Serialization;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeleteCurrentStatement : SqlStatement {
		public DeleteCurrentStatement(ObjectName tableName, string cursorName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			TableName = tableName;
			CursorName = cursorName;
		}

		public ObjectName TableName { get; private set; }

		public string CursorName { get; private set; }

		#region Prepared

		[Serializable]
		class Prepared : SqlStatement {
			public Prepared(ObjectName tableName, string cursorName) {
				TableName = tableName;
				CursorName = cursorName;
			}

			private Prepared(SerializationInfo info, StreamingContext context) {
				TableName = (ObjectName) info.GetValue("TableName", typeof(ObjectName));
				CursorName = info.GetString("CursorName");
			}

			public ObjectName TableName { get; private set; }

			public string CursorName { get; private set; }

			protected override void GetData(SerializationInfo info, StreamingContext context) {
				info.AddValue("TableName", TableName);
				info.AddValue("CursorName", CursorName);
			}
		}

		#endregion
	}
}