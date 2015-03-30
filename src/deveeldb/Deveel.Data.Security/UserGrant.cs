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

using Deveel.Data.Sql;

namespace Deveel.Data.Security {
	/// <summary>
	/// The entity that holds the access control granted to an
	/// user to a specific object in a database.
	/// </summary>
	[Serializable]
	public sealed class UserGrant {
		/// <summary>
		/// Constructs a new grant for the given user on the given
		/// object, including the privileges of the grant.
		/// </summary>
		/// <param name="userName">The name of the user holding the grant.</param>
		/// <param name="privileges">The access privileges granted to the user on the
		/// given object.</param>
		/// <param name="objectName">The fully qualified name of the object on which
		/// to grant the given access privileges to the user.</param>
		/// <param name="objectType">The <see cref="DbObjectType">type of the object</see>.</param>
		/// <param name="granterName">The name of the user that granted.</param>
		public UserGrant(string userName, Privileges privileges, ObjectName objectName, DbObjectType objectType, string granterName) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(granterName))
				throw new ArgumentNullException("granterName");
			if (objectName == null)
				throw new ArgumentNullException("objectName");

			UserName = userName;
			Privileges = privileges;
			ObjectName = objectName;
			ObjectType = objectType;
			GranterName = granterName;
		}

		/// <summary>
		/// Gets the name of the user that holds the access privileges of this grant.
		/// </summary>
		public string UserName { get; private set; }

		/// <summary>
		/// Gets the name of the user that provided this grant.
		/// </summary>
		public string GranterName { get; private set; }

		/// <summary>
		/// Gets the fully qualified name of the object on which this
		/// grant provides access privileges to the user.
		/// </summary>
		/// <remarks>
		/// <para>
		/// The name of the object respects the <c>wildcard rule</c>, that
		/// means if the object own name is a wildcard (<c>*</c>), this grant
		/// will provide access to all objects in the containing schema for the
		/// type given.
		/// </para>
		/// </remarks>
		public ObjectName ObjectName { get; private set; }

		/// <summary>
		/// Gets the type of the object on which to provide access privilleges to 
		/// the user.
		/// </summary>
		public DbObjectType ObjectType { get; private set; }

		/// <summary>
		/// Gets the access privileges granted to the user.
		/// </summary>
		public Privileges Privileges { get; private set; }
	}
}