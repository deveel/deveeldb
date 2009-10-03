// 
//  CommitStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The statements that represents a <c>COMMIT</c> command.
	/// </summary>
	public sealed class CommitStatement : Statement {
		public override void Prepare() {
			// nothing to prepare...
		}

		public override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(database);
			//      try {
			// Commit the current transaction on this connection.
			database.Commit();
			//      }
			//      catch (TransactionException e) {
			//        // This needs to be handled better!
			//        Debug.WriteException(e);
			//        throw new DatabaseException(e.Message);
			//      }
			return FunctionTable.ResultTable(context, 0);
		}
	}
}