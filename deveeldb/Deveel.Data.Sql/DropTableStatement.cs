//  
//  DropTableStatement.cs
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
using System.Collections;

namespace Deveel.Data.Sql {
	///<summary>
	/// The logic of the <c>DROP TABLE</c> SQL command.
	///</summary>
	public class DropTableStatement : Statement {
		/// <summary>
		/// Only create if table doesn't exist.
		/// </summary>
		private bool only_if_exists;

		/// <summary>
		/// The list of tables to drop.
		/// </summary>
		private IList drop_tables = new ArrayList();


		// ---------- Implemented from Statement ----------

		/// <inheritdoc/>
		internal override void Prepare() {
			only_if_exists = GetBoolean("only_if_exists");
			drop_tables = GetList("table_list");

			// Check there are no duplicate entries in the list of tables to drop
			for (int i = 0; i < drop_tables.Count; ++i) {
				Object check = drop_tables[i];
				for (int n = i + 1; n < drop_tables.Count; ++n) {
					if (drop_tables[n].Equals(check)) {
						throw new DatabaseException("Duplicate table in drop: " + check);
					}
				}
			}
		}

		/// <inheritdoc/>
		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			int list_size = drop_tables.Count;
			ArrayList resolved_tables = new ArrayList(list_size);
			// Check the user has privs to delete these tables...
			for (int i = 0; i < list_size; ++i) {
				String table_name = drop_tables[i].ToString();
				TableName tname = ResolveTableName(table_name, Connection);
				// Does the table exist?
				if (!only_if_exists && !Connection.TableExists(tname)) {
					throw new DatabaseException("Table '" + tname + "' does not exist.");
				}

				resolved_tables.Add(tname);
				// Does the user have privs to drop this tables?
				if (!Connection.Database.CanUserDropTableObject(context,
																   User, tname)) {
					throw new UserAccessException(
					   "User not permitted to drop table: " + tname);
				}
			}

			// Check there are no referential links to any tables being dropped
			for (int i = 0; i < list_size; ++i) {
				TableName tname = (TableName)resolved_tables[i];
				// Any tables that have a referential link to this table.
				Transaction.ColumnGroupReference[] refs =
								Connection.QueryTableImportedForeignKeyReferences(tname);
				for (int n = 0; n < refs.Length; ++n) {
					// If the key table isn't being dropped then error
					if (!resolved_tables.Contains(refs[n].key_table_name)) {
						throw new DatabaseConstraintViolationException(
						  DatabaseConstraintViolationException.DropTableViolation,
							"Constraint violation (" + refs[n].name + ") dropping table " +
							tname + " because of referential link from " +
							refs[n].key_table_name);
					}
				}
			}


			// If the 'only if exists' flag is false, we need to check tables to drop
			// exist first.
			if (!only_if_exists) {
				// For each table to drop.
				for (int i = 0; i < list_size; ++i) {
					// Does the table already exist?
					//        String table_name = drop_tables.get(i).toString();
					////        TableName tname =
					////               TableName.Resolve(Connection.CurrentSchema, table_name);
					//        TableName tname = ResolveTableName(table_name, Connection);
					TableName tname = (TableName)resolved_tables[i];

					// If table doesn't exist, throw an error
					if (!Connection.TableExists(tname)) {
						throw new DatabaseException("Can not drop table '" + tname +
													"'.  It does not exist.");
					}
				}
			}

			// For each table to drop.
			int dropped_table_count = 0;
			GrantManager grant_manager = Connection.GrantManager;
			for (int i = 0; i < list_size; ++i) {
				// Does the table already exist?
				//      String table_name = drop_tables.get(i).toString();
				//      TableName tname = ResolveTableName(table_name, Connection);
				TableName tname = (TableName)resolved_tables[i];
				if (Connection.TableExists(tname)) {
					// Drop table in the transaction
					Connection.DropTable(tname);
					// Drop the grants for this object
					grant_manager.RevokeAllGrantsOnObject(
												  GrantObject.Table, tname.ToString());
					// Drop all constraints from the schema
					Connection.DropAllConstraintsForTable(tname);
					++dropped_table_count;
				}
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}