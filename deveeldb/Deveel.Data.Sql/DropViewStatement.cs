//  
//  DropViewStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

		internal override void Prepare() {
			view_name = GetString("view_name");

			String schema_name = Connection.CurrentSchema;
			vname = TableName.Resolve(schema_name, view_name);
			vname = Connection.TryResolveCase(vname);
		}

		internal override Table Evaluate() {
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