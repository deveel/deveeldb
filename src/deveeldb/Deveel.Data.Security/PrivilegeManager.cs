// 
//  Copyright 2010-2015 Deveel
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
//


using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Security {
	public class PrivilegeManager : IPrivilegeManager {
		public PrivilegeManager(ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
		}

		~PrivilegeManager() {
			Dispose(false);
		}

		public ISession Session { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			Session = null;
		}

		private static void UpdateGrants(IQuery queryContext, IMutableTable grantTable, DbObjectType objectType,
			ObjectName objectName,
			string granter, string grantee, Privileges privileges, bool withOption) {
			RevokeAllGrants(queryContext, grantTable, objectType, objectName, granter, grantee, withOption);

			if (privileges != Privileges.None) {
				// Add the grant to the grants table.
				var row = grantTable.NewRow();
				row.SetValue(0, (int) privileges);
				row.SetValue(1, (int) objectType);
				row.SetValue(2, objectName.FullName);
				row.SetValue(3, grantee);
				row.SetValue(4, withOption);
				row.SetValue(5, granter);
				grantTable.AddRow(row);
			}
		}

		private static void RevokeAllGrants(IQuery queryContext, IMutableTable grantTable, DbObjectType objectType,
			ObjectName objectName, string revoker, string grantee, bool withOption = false) {
			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(queryContext, paramCol, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// user or role, and that match the object type.

			// Expression: ("grantee_col" = grantee)
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
				SqlExpression.Constant(Field.String(grantee)));

			// Expression: ("object_col" = object AND
			//              "grantee_col" = grantee)
			// All that match the given grantee or public and given object
			var expr =
				SqlExpression.And(
					SqlExpression.Equal(SqlExpression.Reference(objectCol),
						SqlExpression.Constant(Field.BigInt((int) objectType))), userCheck);

			// Are we only searching for grant options?
			var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
				SqlExpression.Constant(Field.Boolean(withOption)));
			expr = SqlExpression.And(expr, grantOptionCheck);

			// Make sure the granter matches up also
			var granterCheck = SqlExpression.Equal(SqlExpression.Reference(granterCol),
				SqlExpression.Constant(Field.String(revoker)));
			expr = SqlExpression.And(expr, granterCheck);

			t1 = t1.ExhaustiveSelect(queryContext, expr);

			// Remove these rows from the table
			grantTable.Delete(t1);
		}


		private void UpdateUserGrants(DbObjectType objectType, ObjectName objectName, string granter, string grantee,
			Privileges privileges, bool withOption) {
			using (var query = Session.CreateQuery()) {
				var grantTable = query.Access.GetMutableTable(SystemSchema.GrantsTableName);

				UpdateGrants(query, grantTable, objectType, objectName, granter, grantee, privileges, withOption);
			}
		}

		public void Grant(Grant grant) {
			if (grant == null)
				throw new ArgumentNullException("grant");

			var objectType = grant.ObjectType;
			var objectName = grant.ObjectName;
			var privileges = grant.Privileges;

			Privileges oldPrivs = GetPrivileges(grant.Grantee, objectType, objectName, grant.WithOption);
			privileges |= oldPrivs;

			if (!oldPrivs.Equals(privileges))
				UpdateUserGrants(objectType, objectName, grant.GranterName, grant.Grantee, privileges, grant.WithOption);
		}


		private void RevokeAllGrantsFrom(DbObjectType objectType, ObjectName objectName, string revoker, string grantee,
			bool withOption = false) {
			using (var query = Session.CreateQuery()) {
				var grantTable = query.Access.GetMutableTable(SystemSchema.GrantsTableName);

				var objectCol = grantTable.GetResolvedColumnName(1);
				var paramCol = grantTable.GetResolvedColumnName(2);
				var granteeCol = grantTable.GetResolvedColumnName(3);
				var grantOptionCol = grantTable.GetResolvedColumnName(4);
				var granterCol = grantTable.GetResolvedColumnName(5);

				ITable t1 = grantTable;

				// All that match the given object parameter
				// It's most likely this will reduce the search by the most so we do
				// it first.
				t1 = t1.SimpleSelect(query, paramCol, SqlExpressionType.Equal,
					SqlExpression.Constant(Field.String(objectName.FullName)));

				// The next is a single exhaustive select through the remaining records.
				// It finds all grants that match either public or the grantee is the
				// user or role, and that match the object type.

				// Expression: ("grantee_col" = grantee)
				var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
					SqlExpression.Constant(Field.String(grantee)));

				// Expression: ("object_col" = object AND
				//              "grantee_col" = grantee)
				// All that match the given grantee or public and given object
				var expr =
					SqlExpression.And(
						SqlExpression.Equal(SqlExpression.Reference(objectCol),
							SqlExpression.Constant(Field.BigInt((int) objectType))), userCheck);

				// Are we only searching for grant options?
				var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
					SqlExpression.Constant(Field.Boolean(withOption)));
				expr = SqlExpression.And(expr, grantOptionCheck);

				// Make sure the granter matches up also
				var granterCheck = SqlExpression.Equal(SqlExpression.Reference(granterCol),
					SqlExpression.Constant(Field.String(revoker)));
				expr = SqlExpression.And(expr, granterCheck);

				t1 = t1.ExhaustiveSelect(query, expr);

				// Remove these rows from the table
				grantTable.Delete(t1);
			}
		}

		public Grant[] GetGrants(string grantee, bool withPublic) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.GrantsTableName);

				var granteeColumn = table.GetResolvedColumnName(3);

				ITable t1 = table;

				// The next is a single exhaustive select through the remaining records.
				// It finds all grants that match either public or the grantee is the
				// user or role, and that match the object type.

				// Expression: ("grantee_col" = grantee OR "grantee_col" = 'public')
				var granteeCheck = SqlExpression.Equal(SqlExpression.Reference(granteeColumn),
					SqlExpression.Constant(Field.String(grantee)));
				if (withPublic) {
					granteeCheck = SqlExpression.Or(granteeCheck, SqlExpression.Equal(SqlExpression.Reference(granteeColumn),
						SqlExpression.Constant(Field.String(User.PublicName))));
				}

				t1 = t1.ExhaustiveSelect(query, granteeCheck);

				var list = new List<Grant>();

				foreach (var row in t1) {
					var privBit = (Privileges) ((SqlNumber) row.GetValue(0).Value).ToInt32();
					var objType = (DbObjectType) ((SqlNumber) row.GetValue(1).Value).ToInt32();
					var objName = ObjectName.Parse(row.GetValue(2));
					var withOption = row.GetValue(4);
					var granter = row.GetValue(5);
					
					list.Add(new Grant(privBit, objName, objType, grantee, granter, withOption));
				}

				return list.ToArray();
			}
		}

		private static Privileges QueryPrivileges(IQuery queryContext, ITable grantTable, string grantee,
			DbObjectType objectType, ObjectName objectName, bool withOption, bool withPublic) {
			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(queryContext, paramCol, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// user or role, and that match the object type.

			// Expression: ("grantee_col" = grantee OR "grantee_col" = 'public')
			var granteeCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
				SqlExpression.Constant(Field.String(grantee)));
			if (withPublic) {
				granteeCheck = SqlExpression.Or(granteeCheck, SqlExpression.Equal(SqlExpression.Reference(granteeCol),
					SqlExpression.Constant(Field.String(User.PublicName))));
			}

			// Expression: ("object_col" = object AND
			//              ("grantee_col" = grantee OR "grantee_col" = 'public'))
			// All that match the given grantee or public and given object
			var expr = SqlExpression.And(SqlExpression.Equal(SqlExpression.Reference(objectCol),
				SqlExpression.Constant(Field.BigInt((int) objectType))), granteeCheck);

			// Are we only searching for grant options?
			if (withOption) {
				var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
					SqlExpression.Constant(Field.BooleanTrue));
				expr = SqlExpression.And(expr, grantOptionCheck);
			}

			t1 = t1.ExhaustiveSelect(queryContext, expr);

			// For each grant, merge with the resultant priv object
			Privileges privs = Privileges.None;

			foreach (var row in t1) {
				var priv = (int) row.GetValue(0).AsBigInt();
				privs |= (Privileges) priv;
			}

			return privs;
		}

		private Privileges QueryUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName,
			bool withOption, bool withPublic) {
			using (var query = Session.CreateQuery()) {
				// The system grants table.
				var grantTable = query.Access.GetTable(SystemSchema.GrantsTableName);
				return QueryPrivileges(query, grantTable, userName, objectType, objectName, withOption, withPublic);
			}
		}

		public Privileges GetPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption) {
			return QueryUserPrivileges(userName, objectType, objectName, withOption, true);
		}

		public void Revoke(Grant grant) {
			RevokeAllGrantsFrom(grant.ObjectType, grant.ObjectName, grant.GranterName, grant.Grantee, grant.WithOption);
		}

		//public void RevokeAllGrantsOn(DbObjectType objectType, ObjectName objectName) {
		//	using (var query = Session.CreateQuery()) {
		//		var grantTable = query.Access.GetMutableTable(SystemSchema.GrantsTableName);

		//		var privBitCol = grantTable.GetResolvedColumnName(0);
		//		var objectTypeColumn = grantTable.GetResolvedColumnName(1);
		//		var objectNameColumn = grantTable.GetResolvedColumnName(2);
		//		var granteeCol = grantTable.GetResolvedColumnName(3);
		//		var grantOptionCol = grantTable.GetResolvedColumnName(4);
		//		var granterCol = grantTable.GetResolvedColumnName(5);

		//		// All that match the given object
		//		var t1 = grantTable.SimpleSelect(query, objectTypeColumn, SqlExpressionType.Equal,
		//			SqlExpression.Constant(Field.Integer((int) objectType)));

		//		// All that match the given parameter
		//		t1 = t1.SimpleSelect(query, objectNameColumn, SqlExpressionType.Equal,
		//			SqlExpression.Constant(Field.String(objectName.FullName)));

		//		// Remove these rows from the table
		//		grantTable.Delete(t1);
		//	}
		//}

		public Grant[] GetGrantsOn(DbObjectType objectType, ObjectName objectName) {
			using (var query = Session.CreateQuery()) {
				var grantTable = query.Access.GetMutableTable(SystemSchema.GrantsTableName);

				var objectTypeColumn = grantTable.GetResolvedColumnName(1);
				var objectNameColumn = grantTable.GetResolvedColumnName(2);

				// All that match the given object
				var t1 = grantTable.SimpleSelect(query, objectTypeColumn, SqlExpressionType.Equal,
					SqlExpression.Constant(Field.Integer((int) objectType)));

				// All that match the given parameter
				t1 = t1.SimpleSelect(query, objectNameColumn, SqlExpressionType.Equal,
					SqlExpression.Constant(Field.String(objectName.FullName)));

				var list = new List<Grant>();

				foreach (var row in t1) {
					var priv = (Privileges)((SqlNumber) row.GetValue(0).Value).ToInt32();
					var grantee = row.GetValue(3);
					var grantOption = row.GetValue(4);
					var granter = row.GetValue(5);

					list.Add(new Grant(priv, objectName, objectType, grantee, granter, grantOption));
				}

				return list.ToArray();
			}
		}
	}
}
