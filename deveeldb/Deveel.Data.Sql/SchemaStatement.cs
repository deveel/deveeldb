// 
//  SchemaStatement.cs
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
	///<summary>
	/// Statement container that handles the <c>CREATE SCHEMA</c> 
	/// and <c>DROP SCHEMA</c> statements.
	///</summary>
	public class SchemaStatement : Statement {
		/// <summary>
		/// The type (either 'create' or 'drop').
		/// </summary>
		String type;

		/// <summary>
		/// The name of the schema.
		/// </summary>
		String schema_name;

		// ---------- Implemented from Statement ----------

		public override void Prepare() {
			type = (String)cmd.GetObject("type");
			schema_name = (String)cmd.GetObject("schema_name");
		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			String com = type.ToLower();

			if (!database.Database.CanUserCreateAndDropSchema(context, user, schema_name)) {
				throw new UserAccessException("User not permitted to create or drop schema.");
			}

			// Is this a create schema command?
			if (com.Equals("create")) {
				bool ignore_case = database.IsInCaseInsensitiveMode;
				SchemaDef schema =
							database.ResolveSchemaCase(schema_name, ignore_case);
				if (schema == null) {
					// Create the schema
					database.CreateSchema(schema_name, "USER");
					// Set the default grants for the schema
					database.GrantManager.Grant(Privileges.SchemaAll,
								GrantObject.Schema, schema_name, user.UserName,
								true, Database.InternalSecureUsername);
				} else {
					throw new DatabaseException("Schema '" + schema_name +
												"' already exists.");
				}
			}
				// Is this a drop schema command?
			else if (com.Equals("drop")) {
				bool ignore_case = database.IsInCaseInsensitiveMode;
				SchemaDef schema =
							database.ResolveSchemaCase(schema_name, ignore_case);
				// Only allow user to drop USER typed schemas
				if (schema == null) {
					throw new DatabaseException("Schema '" + schema_name + "' does not exist.");
				} else if (schema.Type.Equals("USER")) {
					// Check if the schema is empty.
					TableName[] all_tables = database.Tables;
					String resolved_schema_name = schema.Name;
					for (int i = 0; i < all_tables.Length; ++i) {
						if (all_tables[i].Schema.Equals(resolved_schema_name)) {
							throw new DatabaseException(
										  "Schema '" + schema_name + "' is not empty.");
						}
					}
					// Drop the schema
					database.DropSchema(schema.Name);
					// Revoke all the grants for the schema
					database.GrantManager.RevokeAllGrantsOnObject(
												   GrantObject.Schema, schema.Name);

				} else {
					throw new DatabaseException(
											 "Can not drop schema '" + schema_name + "'");
				}
			} else {
				throw new DatabaseException("Unrecognised schema command.");
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}