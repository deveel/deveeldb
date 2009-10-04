//  
//  ShutdownStatement.cs
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
	/// <summary>
	/// The <c>SHUTDOWN</c> command statement.
	/// </summary>
	public class ShutdownStatement : Statement {
		/// <summary>
		/// Set to true if this statement is a shutdown statement.
		/// </summary>
		bool shutdown = false;



		// ---------- Implemented from Statement ----------

		public override void Prepare() {
			Object command = cmd.GetObject("command");
			shutdown = command.Equals("shutdown");
		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Is this a shutdown statement?
			if (shutdown == true) {

				// Check the user has privs to shutdown...
				if (!database.Database.CanUserShutDown(context, user)) {
					throw new UserAccessException(
							 "User not permitted to shut down the database.");
				}

				// Shut down the database system.
				database.Database.StartShutDownThread();

				// Return 0 to indicate we going to be closing shop!
				return FunctionTable.ResultTable(context, 0);

			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}