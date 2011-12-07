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

using Deveel.Data.Caching;
using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A class that manages the grants on a database for a given database
	/// session and user.
	/// </summary>
	public class GrantManager {

		/// <summary>
		/// The string representing the public user (privs granted to all users).
		/// </summary>
		public const string PublicUsernameStr = "@PUBLIC";

		/// <summary>
		/// The name of the 'public' username.
		/// </summary>
		/// <remarks>
		/// If a grant is made on 'public' then all users are given the grant.
		/// </remarks>
		public readonly static TObject PublicUsername = TObject.CreateString(PublicUsernameStr);

		// ---------- Members ----------
		/// <summary>
		/// The DatabaseConnection instance.
		/// </summary>
		private readonly DatabaseConnection connection;

		/// <summary>
		/// The IQueryContext instance.
		/// </summary>
		private readonly IQueryContext context;

		/// <summary>
		/// A cache of privileges for the various tables in the database.  This cache
		/// is populated as the user 'visits' a table.
		/// </summary>
		private readonly Cache privCache;

		/// <summary>
		/// Set to true if the grant table is modified in this manager.
		/// </summary>
		private bool grantTableChanged;


		internal GrantManager(DatabaseConnection connection) {
			this.connection = connection;
			context = new DatabaseQueryContext(connection);
			privCache = new MemoryCache(129, 129, 20);

			grantTableChanged = false;

			// Attach a cache backed on the GRANTS table which will invalidate the
			// connection cache whenever the grant table is modified.
			connection.AttachTableBackedCache(new TableBackedCacheImpl(this, Database.SysGrants));
		}

		private class TableBackedCacheImpl : TableBackedCache {
			private readonly GrantManager manager;

			public TableBackedCacheImpl(GrantManager manager, TableName tableName)
				: base(tableName) {
				this.manager = manager;
			}

			protected override void PurgeCache(IntegerVector addedRows, IntegerVector removedRows) {
				// If there were changed then invalidate the cache
				if (manager.grantTableChanged) {
					manager.InvalidateGrantCache();
					manager.grantTableChanged = false;
				}
					// Otherwise, if there were committed added or removed changes also
					// invalidate the cache,
				else if ((addedRows != null && addedRows.Count > 0) ||
						 (removedRows != null && removedRows.Count > 0)) {
					manager.InvalidateGrantCache();
				}
			}
		}

		// ---------- Private priv caching methods ----------

		/// <summary>
		/// Flushes any grant information that's being cached.
		/// </summary>
		private void InvalidateGrantCache() {
			privCache.Clear();
		}

		/// <summary>
		/// Represents a grant query on a particular object, param and user name.
		/// </summary>
		/// <remarks>
		/// This object is designed to be an immutable key in a cache.
		/// </remarks>
		private sealed class GrantQuery {
			private readonly GrantObject obj;
			private readonly String param;
			private readonly String username;
			private readonly int flags;

			internal GrantQuery(GrantObject obj, string param, string username, bool flag1, bool flag2) {
				this.obj = obj;
				this.param = param;
				this.username = username;
				flags = flag1 ? 1 : 0;
				flags = flags | (flag2 ? 2 : 0);
			}

			public override bool Equals(object ob) {
				GrantQuery dest = (GrantQuery)ob;
				return (obj == dest.obj &&
						param.Equals(dest.param) &&
						username.Equals(dest.username) &&
						flags == dest.flags);
			}

			public override int GetHashCode() {
				return (int)obj + param.GetHashCode() + username.GetHashCode() + flags;
			}

		}


		private Privileges GetPrivs(GrantObject obj, string param, string username, bool onlyGrantOptions, string granter, bool includePublicPrivs) {
			// Create the grant query key
			GrantQuery key = new GrantQuery(obj, param, username, onlyGrantOptions, includePublicPrivs);

			// Is the Privileges object for this query already in the cache?
			Privileges privs = (Privileges)privCache.Get(key);
			if (privs == null) {
				// Not in cache so we need to ask database for the information.

				// The system grants table.
				DataTable grantTable = connection.GetTable(Database.SysGrants);

				VariableName objectCol = grantTable.GetResolvedVariable(1);
				VariableName paramCol = grantTable.GetResolvedVariable(2);
				VariableName granteeCol = grantTable.GetResolvedVariable(3);
				VariableName grantOptionCol = grantTable.GetResolvedVariable(4);
				VariableName granterCol = grantTable.GetResolvedVariable(5);
				Operator equals = Operator.Equal;

				Table t1 = grantTable;

				// All that match the given object parameter
				// It's most likely this will reduce the search by the most so we do
				// it first.
				t1 = t1.SimpleSelect(context, paramCol, equals, new Expression(TObject.CreateString(param)));

				// The next is a single exhaustive select through the remaining records.
				// It finds all grants that match either public or the grantee is the
				// username, and that match the object type.

				// Expression: ("grantee_col" = username OR "grantee_col" = 'public')
				Expression userCheck = Expression.Simple(granteeCol, equals, TObject.CreateString(username));
				if (includePublicPrivs)
					userCheck = new Expression(userCheck, Operator.Or, Expression.Simple(granteeCol, equals, PublicUsername));

				// Expression: ("object_col" = object AND
				//              ("grantee_col" = username OR "grantee_col" = 'public'))
				// All that match the given username or public and given object
				Expression expr = new Expression(Expression.Simple(objectCol, equals, TObject.CreateInt4((int) obj)),
				                                 Operator.And, userCheck);

				// Are we only searching for grant options?
				if (onlyGrantOptions) {
					Expression grantOptionCheck = Expression.Simple(grantOptionCol, equals, TObject.CreateString("true"));
					expr = new Expression(expr, Operator.And, grantOptionCheck);
				}

				// Do we need to check for a granter when we looking for privs?
				if (granter != null) {
					Expression granterCheck = Expression.Simple(granterCol, equals, TObject.CreateString(granter));
					expr = new Expression(expr, Operator.And, granterCheck);
				}

				t1 = t1.ExhaustiveSelect(context, expr);

				// For each grant, merge with the resultant priv object
				privs = Privileges.Empty;
				IRowEnumerator e = t1.GetRowEnumerator();
				while (e.MoveNext()) {
					int row_index = e.RowIndex;
					BigNumber priv_bit = (BigNumber)t1.GetCellContents(0, row_index).Object;
					privs = privs.Add(priv_bit.ToInt32());
				}

				// Put the privs object in the cache
				privCache.Set(key, privs);
			}

			return privs;
		}

		/// <summary>
		/// Internal method that sets the privs for the given target, param, grantee,
		/// grant option and granter.
		/// </summary>
		/// <param name="newPrivs"></param>
		/// <param name="obj"></param>
		/// <param name="param"></param>
		/// <param name="grantee"></param>
		/// <param name="grantOption"></param>
		/// <param name="granter"></param>
		/// <remarks>
		/// This first revokes any grants that have been setup for the object, 
		/// and adds a new record with the new grants.
		/// </remarks>
		private void InternalSetPrivs(Privileges newPrivs, GrantObject obj, string param, string grantee, bool grantOption, string granter) {
			// Revoke existing privs on this object for this grantee
			RevokeAllGrantsOnObject(obj, param, grantee, grantOption, granter);

			if (!newPrivs.IsEmpty) {
				// The system grants table.
				DataTable grantTable = connection.GetTable(Database.SysGrants);

				// Add the grant to the grants table.
				DataRow rdat = new DataRow(grantTable);
				rdat.SetValue(0, (BigNumber)newPrivs.ToInt32());
				rdat.SetValue(1, (BigNumber)(int)obj);
				rdat.SetValue(2, param);
				rdat.SetValue(3, grantee);
				rdat.SetValue(4, grantOption ? "true" : "false");
				rdat.SetValue(5, granter);
				grantTable.Add(rdat);

				// Invalidate the privilege cache
				InvalidateGrantCache();

				// Notify that the grant table has changed.
				grantTableChanged = true;
			}
		}

		// ---------- Public methods ----------

		/// <summary>
		/// Adds a grant on the given database object.
		/// </summary>
		/// <param name="privs">Privileges to grant.</param>
		/// <param name="obj">The object to grant</param>
		/// <param name="param">The parameter of the object (eg. the table name)</param>
		/// <param name="grantee">The user name to grant the privileges to.</param>
		/// <param name="grantOption">Indicates if the <paramref name="grantee"/> is allowed
		/// to pass grants to other users.</param>
		/// <param name="granter">The user granting the privileges to the <paramref name="grantee"/>.</param>
		/// <exception cref="StatementException">
		/// If <paramref name="obj"/> is <see cref="GrantObject.Schema"/> or 
		/// <see cref="GrantObject.Schema"/> and the method was unable to find 
		/// it (using <paramref name="param"/> as name).
		/// </exception>
		public void Grant(Privileges privs, GrantObject obj, string param, string grantee, bool grantOption, string granter) {
			if (obj == GrantObject.Table) {
				// Check that the table exists,
				if (!connection.TableExists(TableName.Resolve(param)))
					throw new DatabaseException("Table: " + param + " does not exist.");
			} else if (obj == GrantObject.Schema) {
				// Check that the schema exists.
				if (!connection.SchemaExists(param))
					throw new DatabaseException("Schema: " + param + " does not exist.");
			}

			// Get any existing grants on this object to this grantee
			Privileges existingPrivs = GetPrivs(obj, param, grantee, grantOption, granter, false);
			// Merge the existing privs with the new privs being added.
			Privileges newPrivs = privs.Merge(existingPrivs);

			// If the new_privs are the same as the existing privs, don't bother
			// changing anything.
			if (!newPrivs.Equals(existingPrivs))
				InternalSetPrivs(newPrivs, obj, param, grantee, grantOption, granter);
		}

		/// <summary>
		/// Adds the given privileges to all the tables in the given schema.
		/// </summary>
		/// <param name="schema">The schema where the table to grant belongs.</param>
		/// <param name="privs">The privileges to grant.</param>
		/// <param name="grantee">The user name to grant the privileges to.</param>
		/// <param name="grantOption">Indicates if the <paramref name="grantee"/> is allowed
		/// to pass grants to other users.</param>
		/// <param name="granter">The user granting the privileges to the <paramref name="grantee"/>.</param>
		public void GrantToAllTablesInSchema(string schema, Privileges privs, string grantee, bool grantOption, string granter) {
			// The list of all tables
			TableName[] list = connection.Tables;
			foreach (TableName tname in list) {
				// If the table is in the given schema,
				if (tname.Schema.Equals(schema))
					Grant(privs, GrantObject.Table, tname.ToString(), grantee, grantOption, granter);
			}
		}

		/// <summary>
		/// Removes a grant on the given object.
		/// </summary>
		/// <param name="privs">Privileges to revoke.</param>
		/// <param name="obj">The object from where to revoke the privs.</param>
		/// <param name="param">The parameter of the object (eg. the table name)</param>
		/// <param name="grantee">The name of the user to revoke the privileges from.</param>
		/// <param name="grantOption">Indicates if the <paramref name="grantee"/> is not anymore allowed
		/// (if previously setted) to pass grants to other users.</param>
		/// <param name="granter">The user revoking the privileges from the <paramref name="grantee"/>.</param>
		public void Revoke(Privileges privs, GrantObject obj, string param, string grantee, bool grantOption, string granter) {
			// Get any existing grants on this object to this grantee
			Privileges existingPrivs = GetPrivs(obj, param, grantee, grantOption, granter, false);
			// Remove privs from the the existing privs.
			Privileges newPrivs = existingPrivs.Remove(privs);

			// If the new_privs are the same as the existing privs, don't bother
			// changing anything.
			if (!newPrivs.Equals(existingPrivs))
				InternalSetPrivs(newPrivs, obj, param, grantee, grantOption, granter);
		}

		/// <summary>
		/// Revokes al the grants from a given object for a given user.
		/// </summary>
		/// <param name="obj"></param>
		/// <param name="param"></param>
		/// <param name="grantee"></param>
		/// <param name="grantOption"></param>
		/// <param name="granter"></param>
		public void RevokeAllGrantsOnObject(GrantObject obj, string param, string grantee, bool grantOption, string granter) {
			// The system grants table.
			DataTable grantTable = connection.GetTable(Database.SysGrants);

			VariableName objectCol = grantTable.GetResolvedVariable(1);
			VariableName paramCol = grantTable.GetResolvedVariable(2);
			VariableName granteeCol = grantTable.GetResolvedVariable(3);
			VariableName grantOptionCol = grantTable.GetResolvedVariable(4);
			VariableName granterCol = grantTable.GetResolvedVariable(5);
			Operator equals = Operator.Equal;

			Table t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(context, paramCol, equals, new Expression(TObject.CreateString(param)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username)
			Expression userCheck = Expression.Simple(granteeCol, equals, TObject.CreateString(grantee));
			// Expression: ("object_col" = object AND
			//              "grantee_col" = username)
			// All that match the given username or public and given object
			Expression expr = new Expression(Expression.Simple(objectCol, equals, TObject.CreateInt4((int) obj)), Operator.And,
			                                 userCheck);

			// Are we only searching for grant options?
			Expression grantOptionCheck = Expression.Simple(grantOptionCol, equals, TObject.CreateString(grantOption ? "true" : "false"));
			expr = new Expression(expr, Operator.And, grantOptionCheck);

			// Make sure the granter matches up also
			Expression granterCheck = Expression.Simple(granterCol, equals, TObject.CreateString(granter));
			expr = new Expression(expr, Operator.And, granterCheck);

			t1 = t1.ExhaustiveSelect(context, expr);

			// Remove these rows from the table
			grantTable.Delete(t1);

			// Invalidate the privilege cache
			InvalidateGrantCache();

			// Notify that the grant table has changed.
			grantTableChanged = true;
		}

		///<summary>
		/// Completely removes all privs granted on the given object for all users.
		///</summary>
		///<param name="obj"></param>
		///<param name="param"></param>
		/// <remarks>
		/// This would typically be used when the object is dropped from the database.
		/// </remarks>
		public void RevokeAllGrantsOnObject(GrantObject obj, string param) {
			// The system grants table.
			DataTable grantTable = connection.GetTable(Database.SysGrants);

			VariableName objectCol = grantTable.GetResolvedVariable(1);
			VariableName paramCol = grantTable.GetResolvedVariable(2);
			// All that match the given object
			Table t1 = grantTable.SimpleSelect(context, objectCol, Operator.Equal, new Expression(TObject.CreateInt4((int)obj)));
			// All that match the given parameter
			t1 = t1.SimpleSelect(context, paramCol, Operator.Equal, new Expression(TObject.CreateString(param)));

			// Remove these rows from the table
			grantTable.Delete(t1);

			// Invalidate the privilege cache
			InvalidateGrantCache();

			// Notify that the grant table has changed.
			grantTableChanged = true;
		}

		/// <summary>
		/// Gets the privileges for a given user on the given object.
		/// </summary>
		/// <param name="obj">The object to get the grants.</param>
		/// <param name="param">The parameter used to determine the object.</param>
		/// <param name="username">The name of the user to gte the grants.</param>
		/// <remarks>
		/// This would be used to determine the access a user has to a table.
		/// <para>
		/// Note that the Privileges object includes all the grants on the 
		/// object given to PUBLIC also.
		/// </para>
		/// <para>
		/// This method will concatenate multiple privs granted on the same
		/// object.
		/// </para>
		/// <para>
		/// <b>Performances:</b> This method is called a lot (at least once on every query).
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a set of <see cref="Privileges"/> for the given <paramref name="username"/>
		/// on the given object.
		/// </returns>
		public Privileges GetUserGrants(GrantObject obj, string param, string username) {
			return GetPrivs(obj, param, username, false, null, true);
		}

		/// <summary>
		/// Gets the privileges on the given object the given user can grant
		/// to other users.
		/// </summary>
		/// <param name="obj">The object to get the grants.</param>
		/// <param name="param">The parameter used to determine the object.</param>
		/// <param name="username">The name of the user to gte the grants.</param>
		/// <remarks>
		/// This would be used to determine if a user has privileges to give 
		/// another user grants on an object.
		/// <para>
		/// Note that the <see cref="Privileges"/> includes all the grants 
		/// on the object given to PUBLIC also.
		/// </para>
		/// <para>
		/// This method will concatenate multiple grant options given on the same
		/// object to the user.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a set of <see cref="Privileges"/> on the given object 
		/// the user identified by <paramref name="username"/> can pass 
		/// to other users.
		/// </returns>
		public Privileges GetUserGrantOptions(GrantObject obj, string param, string username) {
			return GetPrivs(obj, param, username, true, null, true);
		}
	}
}