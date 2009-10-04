//  
//  CreateSchemaStatement.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	public sealed class CreateSchemaStatement : Statement {
		/// <summary>
		/// The name of the schema.
		/// </summary>
		private string schema_name;

		public override void Prepare() {
			schema_name = (String)cmd.GetObject("schema_name");
		}

		public override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(database);

			if (!database.Database.CanUserCreateAndDropSchema(context, user, schema_name))
				throw new UserAccessException("User not permitted to create or drop schema.");

			bool ignore_case = database.IsInCaseInsensitiveMode;
			SchemaDef schema = database.ResolveSchemaCase(schema_name, ignore_case);
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

			return FunctionTable.ResultTable(context, 0);
		}
	}
}