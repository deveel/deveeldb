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

namespace Deveel.Data.Sql {
	public sealed class DropViewStatement : Statement {
		/// <summary>
		/// The view name to create/drop.
		/// </summary>
		private String view_name;

		/// <summary>
		/// The view name as a TableName object.
		/// </summary>
		private TableName vname;


		#region Overrides of Statement

		protected override void Prepare() {
			view_name = GetString("view_name");

			String schema_name = Connection.CurrentSchema;
			vname = TableName.Resolve(schema_name, view_name);
			vname = Connection.TryResolveCase(vname);
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the user have privs to drop this tables?
			if (!Connection.Database.CanUserDropTableObject(context, User, vname))
				throw new UserAccessException("User not permitted to drop view: " + view_name);

			// Drop the view object
			Connection.DropView(vname);

			// Drop the grants for this object
			Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, vname.ToString());

			return FunctionTable.ResultTable(context, 0);
		}

		#endregion
	}
}