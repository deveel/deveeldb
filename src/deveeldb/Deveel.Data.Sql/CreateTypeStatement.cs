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
	public sealed class CreateTypeStatement : Statement {
		private string type_name;
		private string parent_type_name;

		private TableName resolved_type_name;
		private TableName res_parent_type_name;

		private UserTypeAttributes type_attributes;

		private IList attributes;

		private IList functions;

		private UserType parent;

		#region Overrides of Statement

		protected override void Prepare(IQueryContext context) {
			type_name = GetString("type_name");

			parent_type_name = GetString("parent_type");

			bool final = GetBoolean("final");
			if (final)
				type_attributes |= UserTypeAttributes.Sealed;

			bool external = GetBoolean("external");

			IList members = GetList("members");

			string schema_name = context.Connection.CurrentSchema;
			resolved_type_name = TableName.Resolve(schema_name, type_name);

			if (!String.IsNullOrEmpty(parent_type_name)) {
				if (external)
					throw new Exception("External types not supported yet.");

				res_parent_type_name = TableName.Resolve(schema_name, parent_type_name);
				parent = context.Connection.GetUserType(res_parent_type_name);

				if (parent == null)
					throw new DatabaseException("Unable to find parent type '" + parent_type_name + "'.");
			}

			string name_strip = resolved_type_name.Name;

			if (name_strip.IndexOf('.') != -1)
				throw new DatabaseException("Type name can not contain '.' character.");

			ColumnChecker checker = new ColumnCheckerImpl(members, Connection.IsInCaseInsensitiveMode);

			int sz = members.Count;
			for (int i = 0; i < sz; i++) {
				object member = members[i];
				if (member is SqlTypeAttribute) {
					SqlTypeAttribute sql_attr = (SqlTypeAttribute) member;

					string name = checker.ResolveColumnName(sql_attr.Name);

					UserTypeAttribute attr = new UserTypeAttribute(name, sql_attr.Type);
					attr.Nullable = !sql_attr.NotNull;

					if (attributes == null)
						attributes = new ArrayList();

					attributes.Add(attr);
				} else {
					throw new NotImplementedException();
				}
			}
		}

		protected override Table Evaluate(IQueryContext context) {
			// Does the user have privs to create this type?
			if (!Connection.Database.CanUserCreateTableObject(context, User, resolved_type_name))
				throw new UserAccessException("User not permitted to create table: " + type_name);

			if (Connection.UserTypeExists(resolved_type_name))
				throw new Exception("The type '" + type_name + "' already exists.");

			UserType userType = CreateUserType();
			Connection.CreateUserType(userType);

			Connection.GrantManager.Grant(Privileges.TableAll, GrantObject.Table, resolved_type_name.ToString(), User.UserName,
			                              true, Database.InternalSecureUsername);

			return FunctionTable.ResultTable(context, 0);
		}

		private UserType CreateUserType() {
			UserType userType = new UserType(parent, resolved_type_name, type_attributes);

			for (int i = 0; i < attributes.Count; i++) {
				UserTypeAttribute attribute = (UserTypeAttribute) attributes[i];
				attribute.userType = userType;
			}

			//TODO: add functions...

			return userType;
		}

		#endregion

		#region ColumnCheckerImpl

		private class ColumnCheckerImpl : ColumnChecker {
			private readonly bool ignores_case;
			private readonly IList members;

			public ColumnCheckerImpl(IList members, bool ignoresCase) {
				this.members = members;
				ignores_case = ignoresCase;
			}

			#region Overrides of ColumnChecker

			public override string ResolveColumnName(string columnName) {
				string found_memebr = null;

				for (int i = 0; i < members.Count; i++) {
					string memberName;
					object member = members[i];
					if (member is SqlTypeAttribute)
						memberName = ((SqlTypeAttribute)member).Name;
					else
						throw new NotSupportedException();

					if (string.Compare(memberName, columnName, ignores_case) == 0) {
						if (found_memebr != null)
							throw new Exception("Ambigous member name '" + memberName + "'.");

						found_memebr = memberName;
					}
				}

				return found_memebr;
			}

			#endregion
		}

		#endregion
	}
}