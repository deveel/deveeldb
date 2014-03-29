// 
//  Copyright 2011 Deveel
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

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	[Serializable]
	public sealed class DropSequenceStatement : Statement {
		protected override Table Evaluate(IQueryContext context) {
			string seqNameString = GetString("seq_name");
			TableName seqName = ResolveTableName(context, seqNameString);

			// Does the user have privs to create this sequence generator?
			if (!context.Connection.Database.CanUserDropSequenceObject(context, seqName))
				throw new UserAccessException("User not permitted to drop sequence: " + seqName);

			context.Connection.DropSequenceGenerator(seqName);

			// Drop the grants for this object
			context.Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, seqName.ToString());

			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}