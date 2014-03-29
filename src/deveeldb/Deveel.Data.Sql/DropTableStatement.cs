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
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;

namespace Deveel.Data.Sql {
	///<summary>
	/// The logic of the <c>DROP TABLE</c> SQL command.
	///</summary>
	[Serializable]
	public class DropTableStatement : Statement {
		/// <summary>
		/// Only create if table doesn't exist.
		/// </summary>
		private bool onlyIfExists;

		/// <summary>
		/// The list of tables to drop.
		/// </summary>
		private IList dropTables = new ArrayList();


		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		protected override void Prepare(IQueryContext context) {
			onlyIfExists = GetBoolean("only_if_exists");
			dropTables = GetList("table_list");

			// Check there are no duplicate entries in the list of tables to drop
			for (int i = 0; i < dropTables.Count; ++i) {
				Object check = dropTables[i];
				for (int n = i + 1; n < dropTables.Count; ++n) {
					if (dropTables[n].Equals(check)) {
						throw new DatabaseException("Duplicate table in drop: " + check);
					}
				}
			}
		}

		/// <inheritdoc/>
		protected override Table Evaluate(IQueryContext context) {
			int listSize = dropTables.Count;
			List<TableName> resolvedTables = new List<TableName>(listSize);

			// Check the user has privs to delete these tables...
			for (int i = 0; i < listSize; ++i) {
				string tableNameString = dropTables[i].ToString();
				TableName tname = ResolveTableName(context, tableNameString);

				// Does the table exist?
				if (!onlyIfExists && !context.Connection.TableExists(tname))
					throw new DatabaseException("Table '" + tname + "' does not exist.");

				resolvedTables.Add(tname);

				// Does the user have privs to drop this tables?
				if (!context.Connection.Database.CanUserDropTableObject(context, tname))
					throw new UserAccessException("User not permitted to drop table: " + tname);
			}

			// Check there are no referential links to any tables being dropped
			foreach (TableName tname in resolvedTables) {
				// Any tables that have a referential link to this table.
				DataConstraintInfo[] refs = context.Connection.QueryTableImportedForeignKeyReferences(tname);
				foreach (DataConstraintInfo reference in refs) {
					// If the key table isn't being dropped then error
					if (!resolvedTables.Contains(reference.TableName)) {
						throw new DatabaseConstraintViolationException(
						  DatabaseConstraintViolationException.DropTableViolation,
							"Constraint violation (" + reference.Name + ") dropping table " +
							tname + " because of referential link from " +
							reference.TableName);
					}
				}
			}


			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!onlyIfExists) {
				// For each table to drop.
				foreach (TableName tname in resolvedTables) {
					// If table doesn't exist, throw an error
					if (!context.Connection.TableExists(tname)) {
						throw new DatabaseException("Can not drop table '" + tname +
													"'.  It does not exist.");
					}
				}
			}

			// For each table to drop.
			int droppedTableCount = 0;
			GrantManager grantManager = context.Connection.GrantManager;
			foreach (TableName tname in resolvedTables) {
				// Does the table already exist?
				if (context.Connection.TableExists(tname)) {
					// Drop table in the transaction
					context.Connection.DropTable(tname);

					// Drop the grants for this object
					grantManager.RevokeAllGrantsOnObject(GrantObject.Table, tname.ToString());

					// Drop all constraints from the schema
					context.Connection.DropAllConstraintsForTable(tname);
					++droppedTableCount;
				}
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}