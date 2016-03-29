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

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class DeleteCurrentStatement : SqlStatement, IPlSqlStatement {
		public DeleteCurrentStatement(ObjectName tableName, string cursorName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			TableName = tableName;
			CursorName = cursorName;
		}

		private DeleteCurrentStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			TableName = (ObjectName) info.GetValue("TableName", typeof (ObjectName));
			CursorName = info.GetString("Cursor");
		}

		public ObjectName TableName { get; private set; }

		public string CursorName { get; private set; }

		protected override void GetData(SerializationInfo info) {
			info.AddValue("TableName", TableName);
			info.AddValue("Cursor", CursorName);
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			var tableName = context.Access.ResolveTableName(TableName);
			return new DeleteCurrentStatement(tableName, CursorName);
		}

		protected override void ExecuteStatement(ExecutionContext context) {
			if (!context.DirectAccess.TableExists(TableName))
				throw new ObjectNotFoundException(TableName);
			if (!context.User.CanDeleteFromTable(TableName))
				throw new MissingPrivilegesException(context.User.Name, TableName, Privileges.Delete);

			if (!context.Request.Context.CursorExists(CursorName))
				throw new StatementException(String.Format("The cursor '{0}' was not found in this scope.", CursorName));

			var table = context.DirectAccess.GetMutableTable(TableName);
			if (table == null)
				throw new StatementException(String.Format("The table '{0}' was not found or it is not mutable.", TableName));

			var cursor = context.Request.Context.FindCursor(CursorName);

			cursor.DeleteCurrent(table, context.Request);
		}
	}
}