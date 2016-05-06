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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Cursors;

namespace Deveel.Data.Sql.Statements {
	public sealed class UpdateFromCursorStatement : SqlStatement, IPlSqlStatement {
		public UpdateFromCursorStatement(ObjectName tableName, string cursorName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			TableName = tableName;
			CursorName = cursorName;
		}

		public ObjectName TableName { get; private set; }

		public string CursorName { get; private set; }

		protected override SqlStatement PrepareStatement(IRequest context) {
			var cursor = context.Context.FindCursor(CursorName);
			if (cursor == null)
				throw new ObjectNotFoundException(new ObjectName(CursorName), "The source cursor was not found.");

			throw new NotImplementedException();
		}
	}
}
