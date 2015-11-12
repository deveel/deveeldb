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

using Deveel.Data;
using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	/// <summary>
	/// Provides the information for a user in a database system
	/// </summary>
	public sealed class User {
		public readonly static User System = new User(SystemName);
		public readonly static User Public = new User(PublicName);

		private Dictionary<ObjectName, Privileges> grantCache;

		/// <summary>
		/// Constructs a new user with the given name.
		/// </summary>
		/// <param name="name"></param>
		private User(string name) 
			: this(null, name) {
		}

		/// <summary>
		/// Constructs a new user with the given name.
		/// </summary>
		/// <param name="name"></param>
		internal User(IQueryContext context, string name) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Context = context;
			Name = name;
		}

		/// <summary>
		/// The name of the <c>PUBLIC</c> special user.
		/// </summary>
		public const string PublicName = "@PUBLIC";

		/// <summary>
		/// The name of the <c>SYSTEM</c> special user.
		/// </summary>
		public const string SystemName = "@SYSTEM";

		/// <summary>
		/// Gets the name that uniquely identify a user within a database system.
		/// </summary>
		public string Name { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if this user represents the
		/// <c>SYSTEM</c> special user.
		/// </summary>
		/// <seealso cref="SystemName"/>
		public bool IsSystem {
			get { return Name.Equals(SystemName); }
		}

		/// <summary>
		/// Gets a boolean value indicating if this user represents the
		/// <c>PUBLIC</c> special user.
		/// </summary>
		public bool IsPublic {
			get { return Name.Equals(PublicName); }
		}

		/// <summary>
		/// Gets the query context used to obtain this user object.
		/// </summary>
		/// <remarks>
		/// In some special cases, this is <c>null</c> (for example
		/// for the <see cref="System"/> or <see cref="Public"/> users).
		/// </remarks>
		/// <value>
		/// The query context of the user.
		/// </value>
		public IQueryContext Context { get; private set; }

		/// <summary>
		/// Gets a value indicating whether this user is authenticated.
		/// </summary>
		/// <value>
		/// <c>true</c> if this user is authenticated; otherwise, <c>false</c>.
		/// </value>
		public bool IsAuthenticated {
			get {
				return IsSystem || 
				IsPublic || 
				(Context != null && Context.UserName().Equals(Name, StringComparison.OrdinalIgnoreCase)); }
		}

		/// <summary>
		/// Gets a value indicating whether this user has secure access
		/// over the contextual database.
		/// </summary>
		/// <value>
		/// <c>true</c> if this user has secure access over the database; 
		/// otherwise, <c>false</c>.
		/// </value>
		/// <seealso cref="SecurityQueryExtensions.UserHasSecureAccess(IQueryContext, string)"/>
		public bool HasSecureAccess {
			get {
				if (IsSystem)
					return true;
				if (!IsAuthenticated)
					return false;

				return Context.UserHasSecureAccess();
			}
		}

		/// <summary>
		/// Gets a list of the groups to which the user belongs.
		/// </summary>
		/// <value>
		/// The groups where the user belongs.
		/// </value>
		public string[] Groups {
			get {
				if (IsSystem)
					return new string[0];

				return Context.GetGroupsUserBelongsTo(Name);
			}
		}

		/// <summary>
		/// Determines whether the user has privileges over the specified object.
		/// </summary>
		/// <param name="objectType">Type of the database object.</param>
		/// <param name="objectName">Name of the object to check.</param>
		/// <param name="privileges">The privileges to check.</param>
		/// <returns>
		/// Returns <c>true</c> if this user has the given privileges over the
		/// object of the given type having the given name, <c>false</c> otherwise.
		/// </returns>
		public bool HasPrivileges(DbObjectType objectType, ObjectName objectName, Privileges privileges) {
			if (!IsAuthenticated && !IsSystem)
				return false;

			return Context.UserHasPrivilege(objectType, objectName, privileges);
		}

		/// <summary>
		/// Determines whether this user can create in the schema given.
		/// </summary>
		/// <param name="schemaName">The name of the schema to verify.</param>
		/// <returns></returns>
		public bool CanCreateInSchema(ObjectName schemaName) {
			return HasPrivileges(DbObjectType.Schema, schemaName, Privileges.Create);
		}

		internal bool TryGetObjectGrant(ObjectName objectName, out Privileges grant) {
			if (grantCache == null) {
				grant = Privileges.None;
				return false;
			}

			return grantCache.TryGetValue(objectName, out grant);
		}

		internal void CacheObjectGrant(ObjectName objectName, Privileges grant) {
			if (grantCache == null)
				grantCache = new Dictionary<ObjectName, Privileges>();

			grantCache[objectName] = grant;
		}

		internal void ClearGrantCache(ObjectName objName) {
			if (grantCache.Remove(objName)) {
				if (grantCache.Count == 0)
					grantCache = null;
			}
		}
	}
}