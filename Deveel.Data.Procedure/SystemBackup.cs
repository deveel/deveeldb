// 
//  SystemBackup.cs
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
using System.IO;

namespace Deveel.Data.Procedure {
	///<summary>
	/// A stored procedure that backs up the entire database to the given 
	/// directory in the file system.
	///</summary>
	/// <remarks>
	/// Requires one parameter, the locate to back up the database to.
	/// </remarks>
	public class SystemBackup {

		/**
		 * The stored procedure invokation method.
		 */
		public static String invoke(IProcedureConnection db_connection,
									String path) {

			if (!Directory.Exists(path)) {
				throw new ProcedureException("Path '" + path +
											 "' doesn't exist or is not a directory.");
			}

			try {
				db_connection.Database.LiveCopyTo(path);
				return path;
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message); 
				Console.Error.WriteLine(e.StackTrace);
				throw new ProcedureException("IO Error: " + e.Message);
			}

		}

	}
}