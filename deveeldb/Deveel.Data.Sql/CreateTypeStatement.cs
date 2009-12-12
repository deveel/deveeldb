//  
//  CreateTableStatement.cs
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

		internal override void Prepare() {
			type_name = GetString("type_name");

			parent_type_name = GetString("parent_type");

			bool final = GetBoolean("final");
			if (final)
				type_attributes |= UserTypeAttributes.Sealed;

			bool external = GetBoolean("external");

			IList members = GetList("members");

			string schema_name = Connection.CurrentSchema;
			resolved_type_name = TableName.Resolve(schema_name, type_name);

			if (parent_type_name != null && parent_type_name.Length > 0) {
				if (external)
					throw new Exception("External types not yest supported.");

				res_parent_type_name = TableName.Resolve(schema_name, parent_type_name);
				parent = Connection.GetUserType(res_parent_type_name);

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

		internal override Table Evaluate() {
			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

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

			internal override string ResolveColumnName(string col_name) {
				string found_memebr = null;

				for (int i = 0; i < members.Count; i++) {
					string memberName;
					object member = members[i];
					if (member is SqlTypeAttribute)
						memberName = ((SqlTypeAttribute)member).Name;
					else
						throw new NotSupportedException();

					if (string.Compare(memberName, col_name, ignores_case) == 0) {
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