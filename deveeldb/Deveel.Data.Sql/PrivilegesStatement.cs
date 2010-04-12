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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Handler for grant/revoke queries for setting up grant information 
	/// in the database.
	/// </summary>
	public class PrivilegesStatement : Statement {
		// ---------- Implemented from Statement ----------

		protected override void Prepare() {
			// Nothing to do here
		}

		protected override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String command_type = GetString("command");

			IList priv_list = GetList("priv_list");
			string priv_object = GetString("priv_object");

			GrantObject grant_object;
			String grant_param;

			// Parse the priv object,
			if (priv_object.StartsWith("T:")) {
				// Granting to a table object
				String table_name_str = priv_object.Substring(2);
				TableName table_name = Connection.ResolveTableName(table_name_str);
				// Check the table exists
				if (!Connection.TableExists(table_name)) {
					throw new DatabaseException("Table '" +
												table_name + "' doesn't exist.");
				}
				grant_object = GrantObject.Table;
				grant_param = table_name.ToString();
			} else if (priv_object.StartsWith("S:")) {
				// Granting to a schema object
				String schema_name_str = priv_object.Substring(2);
				SchemaDef schema_name = Connection.ResolveSchemaName(schema_name_str);
				// Check the schema exists
				if (schema_name == null ||
					!Connection.SchemaExists(schema_name.ToString())) {
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
				IList grant_to = GetList("grant_to");
				bool grant_option = GetBoolean("grant_option");

				// Get the grant manager.
				GrantManager manager = context.GrantManager;

				// Get the grant options this user has on the given object.
				Privileges options_privs = manager.GetUserGrantOptions(
										 grant_object, grant_param, User.UserName);

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
						!Connection.Database.UserExists(context, name)) {
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
										 grant_option, User.UserName);
					} else {
						// Add a user grant.
						manager.Grant(grant_privs, grant_object, grant_param,
										 name, grant_option, User.UserName);
					}
				}

				// All done.

			} else if (command_type.Equals("REVOKE")) {
				IList revoke_from = GetList("revoke_from");
				bool revoke_grant_option = GetBoolean("revoke_grant_option");

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
											revoke_grant_option, User.UserName);
					} else {
						// Revoke a user grant.
						manager.Revoke(revoke_privs, grant_object, grant_param,
											name, revoke_grant_option, User.UserName);
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