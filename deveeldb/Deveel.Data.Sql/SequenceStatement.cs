//  
//  SequenceStatement.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// A statement tree for creating and dropping sequence generators.
	/// </summary>
	public class SequenceStatement : Statement {

		internal String type;

		internal TableName seq_name;

		internal Expression increment;
		Expression min_value;
		Expression max_value;
		internal Expression start_value;
		Expression cache_value;
		bool cycle;

		// ----------- Implemented from Statement ----------

		internal override void Prepare() {
			type = GetString("type");
			String sname = GetString("seq_name");
			String schema_name = Connection.CurrentSchema;
			seq_name = TableName.Resolve(schema_name, sname);
			seq_name = Connection.TryResolveCase(seq_name);

			if (type.Equals("create")) {
				// Resolve the function name into a TableName object.    
				increment = GetExpression("increment");
				min_value = GetExpression("min_value");
				max_value = GetExpression("max_value");
				start_value = GetExpression("start");
				cache_value = GetExpression("cache");
				cycle = GetValue("cycle") != null;
			}

		}

		internal override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Does the schema exist?
			bool ignore_case = Connection.IsInCaseInsensitiveMode;
			SchemaDef schema =
					  Connection.ResolveSchemaCase(seq_name.Schema, ignore_case);
			if (schema == null) {
				throw new DatabaseException("Schema '" + seq_name.Schema +
											"' doesn't exist.");
			} else {
				seq_name = new TableName(schema.Name, seq_name.Name);
			}

			if (type.Equals("create")) {

				// Does the user have privs to create this sequence generator?
				if (!Connection.Database.CanUserCreateSequenceObject(context,
																		User, seq_name)) {
					throw new UserAccessException(
									"User not permitted to create sequence: " + seq_name);
				}

				// Does a table already exist with this name?
				if (Connection.TableExists(seq_name)) {
					throw new DatabaseException("Database object with name '" + seq_name +
												"' already exists.");
				}

				// Resolve the expressions,
				long v_start_value = 0;
				if (start_value != null) {
					v_start_value =
					   start_value.Evaluate(null, null, context).ToBigNumber().ToInt64();
				}
				long v_increment_by = 1;
				if (increment != null) {
					v_increment_by =
						 increment.Evaluate(null, null, context).ToBigNumber().ToInt64();
				}
				long v_min_value = 0;
				if (min_value != null) {
					v_min_value =
						 min_value.Evaluate(null, null, context).ToBigNumber().ToInt64();
				}
				long v_max_value = Int64.MaxValue;
				if (max_value != null) {
					v_max_value =
						 max_value.Evaluate(null, null, context).ToBigNumber().ToInt64();
				}
				long v_cache = 16;
				if (cache_value != null) {
					v_cache =
					   cache_value.Evaluate(null, null, context).ToBigNumber().ToInt64();
					if (v_cache <= 0) {
						throw new DatabaseException("Cache size can not be <= 0");
					}
				}

				if (v_min_value >= v_max_value) {
					throw new DatabaseException("Min value can not be >= the max value.");
				}
				if (v_start_value < v_min_value ||
					v_start_value >= v_max_value) {
					throw new DatabaseException(
								   "Start value is outside the min/max sequence bounds.");
				}

				Connection.CreateSequenceGenerator(seq_name,
					   v_start_value, v_increment_by, v_min_value, v_max_value,
					   v_cache, cycle);

				// The initial grants for a sequence is to give the user who created it
				// full access.
				Connection.GrantManager.Grant(
					 Privileges.ProcedureAll, GrantObject.Table,
					 seq_name.ToString(), User.UserName, true,
					 Database.InternalSecureUsername);

			} else if (type.Equals("drop")) {

				// Does the user have privs to create this sequence generator?
				if (!Connection.Database.CanUserDropSequenceObject(context,
																	  User, seq_name)) {
					throw new UserAccessException(
									"User not permitted to drop sequence: " + seq_name);
				}

				Connection.DropSequenceGenerator(seq_name);

				// Drop the grants for this object
				Connection.GrantManager.RevokeAllGrantsOnObject(GrantObject.Table, seq_name.ToString());

			} else {
				throw new Exception("Unknown type: " + type);
			}

			// Return an update result table.
			return FunctionTable.ResultTable(context, 0);
		}
	}
}