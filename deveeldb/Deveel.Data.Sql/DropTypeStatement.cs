//  
//  DropTypeStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Sql {
	public sealed class DropTypeStatement : Statement {
		/// <summary>
		/// The list of tables to drop.
		/// </summary>
		private IList drop_types = new ArrayList();

		#region Overrides of Statement

		internal override void Prepare() {
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

		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			int list_size = drop_types.Count;
			ArrayList resolved_types = new ArrayList(list_size);
			// Check the user has privs to delete these tables...
			for (int i = 0; i < list_size; ++i) {
				string type_name = drop_types[i].ToString();
				TableName res_type_name = ResolveTableName(type_name, Connection);
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

		#endregion
	}
}