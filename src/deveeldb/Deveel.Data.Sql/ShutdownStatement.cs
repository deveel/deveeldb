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
	/// <summary>
	/// The <c>SHUTDOWN</c> command statement.
	/// </summary>
	public class ShutdownStatement : Statement {

		// ---------- Implemented from Statement ----------

		protected override void Prepare() {
			// nothing to prepare
		}

		protected override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check the user has privs to shutdown...
			if (!Connection.Database.CanUserShutDown(context, User)) {
				throw new UserAccessException(
						 "User not permitted to shut down the database.");
			}

			// Shut down the database system.
			Connection.Database.StartShutDownThread();

			// Return 0 to indicate we going to be closing shop!
			return FunctionTable.ResultTable(context, 0);
		}
	}
}