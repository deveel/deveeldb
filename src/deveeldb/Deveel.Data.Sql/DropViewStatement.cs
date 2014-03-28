// 
//  Copyright 2010  Deveel
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

using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class DropViewStatement : Statement {
		protected override Table Evaluate(IQueryContext context) {
			string viewNameString = GetString("view_name");

			TableName viewName = ResolveTableName(context, viewNameString);

			// Does the user have privs to drop this tables?
			if (!context.Connection.Database.CanUserDropTableObject(context, viewName))
				throw new UserAccessException("User not permitted to drop view: " + viewNameString);

			// Drop the view object
			context.Connection.DropView(viewName);

			// Drop the grants for this object
			context.Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, viewName.ToString());

			return FunctionTable.ResultTable(context, 0);
		}
	}
}