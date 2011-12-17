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
	public abstract class PrivilegesStatement : Statement {
		internal PrivilegesStatement(Privileges privileges, GrantObject objType, string objName, IList users, bool grantOption) {
			GrantObject = objType;
			GrantObjectName = objName;

			for (int i = 0; i < users.Count; i++)
				Users.Add(users[i]);

			Privileges = privileges;
			GrantOption = grantOption;
		}

		internal PrivilegesStatement() {
		}

		private IList users;
		private IList priv_list;
		private bool grantOption;
		private GrantObject grantObject;
		private string grantName;

		public GrantObject GrantObject {
			get { return (GrantObject) GetValue("grant_object"); }
			set { SetValue("grant_object", value); }
		}

		public string GrantObjectName {
			get { return GetString("grant_name"); }
			set {
				if (String.IsNullOrEmpty(value))
					throw new ArgumentNullException("value");

				SetValue("grant_name", value);
			}
		}

		public bool GrantOption {
			get { return GetBoolean("grant_option"); }
			set { SetValue("grant_option", value); }
		}

		public IList Users {
			get { return GetList("users", true); }
		}

		public Privileges Privileges {
			get {
				IList privs = GetList("priv_list");
				Privileges p = Privileges.Empty;
				for (int i = 0; i < privs.Count; i++) {
					string privName = (string) privs[i];
					p = p.Add(Privileges.ParseString(privName.ToUpper()));
				}

				return p;
			}
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				IList privs = value.ToStringList();
				IList privList = GetList("priv_list", true);
				for (int i = 0; i < privs.Count; i++) {
					privList.Add(privs[i]);
				}
			}
		}

		internal static IList UserList(string userName) {
			ArrayList list = new ArrayList();
			list.Add(userName);
			return list;
		}

		// ---------- Implemented from Statement ----------

		internal abstract void ExecutePrivilegeAction(IQueryContext context, PrivilegeActionInfo actionInfo);

		protected override void Prepare(IQueryContext context) {
			priv_list = GetList("priv_list");
			users = GetList("users");
			string priv_object = GetString("priv_object");
			grantOption = GetBoolean("grant_option");

			// Parse the priv object,
			if (priv_object.StartsWith("T:")) {
				// Granting to a table object
				string tableNameString = priv_object.Substring(2);
				TableName tableName = ResolveTableName(context, tableNameString);
				// Check the table exists
				if (!context.Connection.TableExists(tableName))
					throw new DatabaseException("Table '" + tableName + "' doesn't exist.");

				grantObject = GrantObject.Table;
				grantName = tableName.ToString();
			} else if (priv_object.StartsWith("S:")) {
				// Granting to a schema object
				string schemaNameString = priv_object.Substring(2);
				SchemaDef schemaName = ResolveSchemaName(context, schemaNameString);
				// Check the schema exists
				if (schemaName == null ||
					!context.Connection.SchemaExists(schemaName.ToString())) {
					schemaNameString = schemaName == null ? schemaNameString :
															schemaName.ToString();
					throw new DatabaseException("Schema '" + schemaNameString + "' doesn't exist.");
				}
				grantObject = GrantObject.Schema;
				grantName = schemaName.ToString();
			} else {
				throw new ApplicationException("Priv object formatting error.");
			}
		}

		protected override Table Evaluate(IQueryContext context) {
			/*
			OLD:
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			String command_type = CreateString("command");

			IList priv_list = GetList("priv_list");
			string priv_object = CreateString("priv_object");

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
				IList grant_to = GetList("users");
				bool grant_option = CreateBoolean("grant_option");

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
				IList revoke_from = GetList("users");
				bool revoke_grant_option = CreateBoolean("grant_option");

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
			*/

			// Get the grant options this user has on the given object.
			Privileges optionsPrivs = context.GetUserGrants(grantObject, grantName);

			// Is the user permitted to give out these privs?
			Privileges grantPrivs = Privileges.Empty;
			for (int i = 0; i < priv_list.Count; ++i) {
				String priv = ((String)priv_list[i]).ToUpper();
				int privBit;
				if (priv.Equals("ALL")) {
					if (grantObject == GrantObject.Table) {
						privBit = Privileges.TableAll.ToInt32();
					} else if (grantObject == GrantObject.Schema) {
						privBit = Privileges.SchemaAll.ToInt32();
					} else {
						throw new ApplicationException("Unrecognised grant object.");
					}
				} else {
					privBit = Privileges.ParseString(priv);
				}
				if (!optionsPrivs.Permits(privBit)) {
					throw new UserAccessException("User is not permitted to grant '" + priv + "' access on object " + grantName);
				}
				grantPrivs = grantPrivs.Add(privBit);
			}

			// Do the users exist?
			for (int i = 0; i < users.Count; ++i) {
				string name = (string)users[i];
				if (String.Compare(name, "public", true) != 0 &&
					!context.Connection.Database.UserExists(context, name)) {
					throw new DatabaseException("User '" + name + "' doesn't exist.");
				}
			}

			// Everything checks out so add the grants to the users.
			for (int i = 0; i < users.Count; ++i) {
				string name = (String)users[i];
				ExecutePrivilegeAction(context, new PrivilegeActionInfo(name, grantObject, grantName, grantPrivs, grantOption));
			}

			return FunctionTable.ResultTable(context, 0);
		}

		internal class PrivilegeActionInfo {
			public PrivilegeActionInfo(string user, GrantObject obj, string objName, Privileges priv, bool grantOption) {
				this.obj = obj;
				this.objName = objName;
				this.user = user;
				this.priv = priv;
				this.grantOption = grantOption;
			}

			private readonly Privileges priv;
			private readonly GrantObject obj;
			private readonly string objName;
			private readonly bool grantOption;
			private readonly string user;

			public string User {
				get { return user; }
			}

			public GrantObject Object {
				get { return obj; }
			}

			public string ObjectName {
				get { return objName; }
			}

			public Privileges Privilege {
				get { return priv; }
			}

			public bool GrantOption {
				get { return grantOption; }
			}

			public bool IsPublicUser {
				get { return String.Compare(user, "public", true) == 0; }
			}
		}
	}
}