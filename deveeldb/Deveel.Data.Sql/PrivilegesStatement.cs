// 
//  PrivilegesStatement.cs
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
using System.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Handler for grant/revoke queries for setting up grant information 
	/// in the database.
	/// </summary>
	public class PrivilegesStatement : Statement {
		// ---------- Implemented from Statement ----------

		public override void Prepare() {
			// Nothing to do here
		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			String command_type = (String)cmd.GetObject("command");

			ArrayList priv_list = (ArrayList)cmd.GetObject("priv_list");
			String priv_object = (String)cmd.GetObject("priv_object");

			GrantObject grant_object;
			String grant_param;

			// Parse the priv object,
			if (priv_object.StartsWith("T:")) {
				// Granting to a table object
				String table_name_str = priv_object.Substring(2);
				TableName table_name = database.ResolveTableName(table_name_str);
				// Check the table exists
				if (!database.TableExists(table_name)) {
					throw new DatabaseException("Table '" +
												table_name + "' doesn't exist.");
				}
				grant_object = GrantObject.Table;
				grant_param = table_name.ToString();
			} else if (priv_object.StartsWith("S:")) {
				// Granting to a schema object
				String schema_name_str = priv_object.Substring(2);
				SchemaDef schema_name = database.resolveSchemaName(schema_name_str);
				// Check the schema exists
				if (schema_name == null ||
					!database.SchemaExists(schema_name.ToString())) {
					schema_name_str = schema_name == null ? schema_name_str :
															schema_name.ToString();
					throw new DatabaseException("Schema '" + schema_name_str +
												"' doesn't exist.");
				}
				grant_object = GrantObject.Schema;
				grant_param = schema_name.ToString();
			} else {
				throw new ApplicationException("Priv object formatting error.");
			}

			if (command_type.Equals("GRANT")) {
				ArrayList grant_to = (ArrayList)cmd.GetObject("grant_to");
				bool grant_option = cmd.GetBoolean("grant_option");

				// Get the grant manager.
				GrantManager manager = context.GrantManager;

				// Get the grant options this user has on the given object.
				Privileges options_privs = manager.GetUserGrantOptions(
										 grant_object, grant_param, user.UserName);

				// Is the user permitted to give out these privs?
				Privileges grant_privs = Privileges.Empty;
				for (int i = 0; i < priv_list.Count; ++i) {
					String priv = ((String)priv_list[i]).ToUpper();
					int priv_bit;
					if (priv.Equals("ALL")) {
						if (grant_object == GrantObject.Table) {
							priv_bit = Privileges.TableAll.ToInt32();
						} else if (grant_object == GrantObject.Schema) {
							priv_bit = Privileges.SchemaAll.ToInt32();
						} else {
							throw new ApplicationException("Unrecognised grant object.");
						}
					} else {
						priv_bit = Privileges.ParseString(priv);
					}
					if (!options_privs.Permits(priv_bit)) {
						throw new UserAccessException(
							  "User is not permitted to grant '" + priv +
							  "' access on object " + grant_param);
					}
					grant_privs = grant_privs.Add(priv_bit);
				}

				// Do the users exist?
				for (int i = 0; i < grant_to.Count; ++i) {
					String name = (String)grant_to[i];
					if (String.Compare(name, "public", true) != 0 &&
						!database.Database.UserExists(context, name)) {
						throw new DatabaseException("User '" + name + "' doesn't exist.");
					}
				}

				// Everything checks out so add the grants to the users.
				for (int i = 0; i < grant_to.Count; ++i) {
					String name = (String)grant_to[i];
					if (String.Compare(name, "public", true) == 0) {
						// Add a public grant,
						manager.Grant(grant_privs, grant_object, grant_param,
										 GrantManager.PublicUsernameStr,
										 grant_option, user.UserName);
					} else {
						// Add a user grant.
						manager.Grant(grant_privs, grant_object, grant_param,
										 name, grant_option, user.UserName);
					}
				}

				// All done.

			} else if (command_type.Equals("REVOKE")) {
				ArrayList revoke_from = (ArrayList)cmd.GetObject("revoke_from");
				bool revoke_grant_option = cmd.GetBoolean("revoke_grant_option");

				// Get the grant manager.
				GrantManager manager = context.GrantManager;

				// Is the user permitted to give out these privs?
				Privileges revoke_privs = Privileges.Empty;
				for (int i = 0; i < priv_list.Count; ++i) {
					String priv = ((String)priv_list[i]).ToUpper();
					int priv_bit;
					if (priv.Equals("ALL")) {
						if (grant_object == GrantObject.Table) {
							priv_bit = Privileges.TableAll.ToInt32();
						} else if (grant_object == GrantObject.Schema) {
							priv_bit = Privileges.SchemaAll.ToInt32();
						} else {
							throw new ApplicationException("Unrecognised grant object.");
						}
					} else {
						priv_bit = Privileges.ParseString(priv);
					}
					revoke_privs = revoke_privs.Add(priv_bit);
				}

				// Revoke the grants for the given users
				for (int i = 0; i < revoke_from.Count; ++i) {
					String name = (String)revoke_from[i];
					if (String.Compare(name, "public", true) == 0) {
						// Revoke a public grant,
						manager.Revoke(revoke_privs, grant_object, grant_param,
											GrantManager.PublicUsernameStr,
											revoke_grant_option, user.UserName);
					} else {
						// Revoke a user grant.
						manager.Revoke(revoke_privs, grant_object, grant_param,
											name, revoke_grant_option, user.UserName);
					}
				}

				// All done.

			} else {
				throw new ApplicationException("Unknown priv manager command: " + command_type);
			}

			return FunctionTable.ResultTable(context, 0);
		}
	}
}