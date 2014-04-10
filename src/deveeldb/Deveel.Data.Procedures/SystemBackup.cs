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
using System.IO;

namespace Deveel.Data.Procedures {
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
		public static String Invoke(IProcedureConnection dbConnection, string path) {
			if (!Directory.Exists(path))
				throw new ProcedureException("Path '" + path + "' doesn't exist or is not a directory.");

			try {
				dbConnection.Database.LiveCopyTo(path);
				return path;
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message); 
				Console.Error.WriteLine(e.StackTrace);
				throw new ProcedureException("IO Error: " + e.Message);
			}
		}
	}
}