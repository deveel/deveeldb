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
	/// The statements that represents a <c>COMMIT</c> command.
	/// </summary>
	public sealed class CommitStatement : Statement {
		protected override void Prepare() {
			// nothing to prepare...
		}

		protected override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);
			//      try {
			// Commit the current transaction on this connection.
			Connection.Commit();
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