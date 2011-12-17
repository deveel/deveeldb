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
using System.Collections;

namespace Deveel.Data.Sql {
	public sealed class DropTypeStatement : Statement {
		/// <summary>
		/// The list of tables to drop.
		/// </summary>
		private IList drop_types = new ArrayList();

		protected override void Prepare() {
			drop_types = GetList("type_list");

			// Check there are no duplicate entries in the list of tables to drop
			for (int i = 0; i < drop_types.Count; ++i) {
				object check = drop_types[i];
				for (int n = i + 1; n < drop_types.Count; ++n) {
					if (drop_types[n].Equals(check))
						throw new DatabaseException("Duplicate type in drop: " + check);
				}
			}
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			int list_size = drop_types.Count;
			ArrayList resolved_types = new ArrayList(list_size);
			// Check the user has privs to delete these tables...
			for (int i = 0; i < list_size; ++i) {
				string type_name = drop_types[i].ToString();
				TableName res_type_name = ResolveTableName(type_name);
				// Does the table exist?
				if (!Connection.UserTypeExists(res_type_name))
					throw new DatabaseException("Type '" + res_type_name + "' does not exist.");

				resolved_types.Add(res_type_name);
				// Does the user have privs to drop this tables?
				if (!Connection.Database.CanUserDropTableObject(context, User, res_type_name))
					throw new UserAccessException("User not permitted to drop type: " + res_type_name);
			}

			// For each type to drop.
			for (int i = 0; i < list_size; ++i) {
				// Does the type already exist?
				TableName type_name = (TableName)resolved_types[i];

				// If type doesn't exist, throw an error
				if (!Connection.TableExists(type_name))
					throw new DatabaseException("Can not drop type '" + type_name + "': It does not exist.");
			}

			// For each type to drop.
			int dropped_type_count = 0;
			GrantManager grant_manager = Connection.GrantManager;
			for (int i = 0; i < list_size; ++i) {
				// Does the type already exist?
				TableName tname = (TableName)resolved_types[i];
				if (Connection.UserTypeExists(tname)) {
					// Drop type in the transaction
					Connection.DropUserType(tname);
					// Drop the grants for this object
					grant_manager.RevokeAllGrantsOnObject(GrantObject.Table, tname.ToString());
					++dropped_type_count;
				}
			}

			return FunctionTable.ResultTable(context, dropped_type_count);
		}
	}
}