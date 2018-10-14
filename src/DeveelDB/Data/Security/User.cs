// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Security {
	/// <summary>
	/// The user of a database system that is granted with privileges
	/// and can optionally be included in roles.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A database system can be operated only by users and this requires
	/// a process of authentication: access control layer of the system
	/// uses this identification to discriminate the privileges to
	/// access resources.
	/// </para>
	/// <para>
	/// A single user can have multiple roles associated: the system
	/// will fallback checking for the role privileges if the no privilege
	/// at user-level was found to authorize the access to a resource.
	/// </para>
	/// <para>
	/// The system defines two special users and their names:
	/// <list type="bullet">
	/// <item><description><strong>System</strong>: a special virtual 
	/// user that cannot be authenticated and has all privileges to interact
	/// with any resource of a database</description></item>
	/// <item><description><strong>Public</strong>: a user that impersonates the
	/// public access to the database resources (it can optionally disallowed
	/// by administrators)</description></item>
	/// </list>
	/// </para>
	/// </remarks>
	public sealed class User : IPrivileged {
		/// <summary>
		/// The special name of the SYSTEM user
		/// </summary>
		public const string SystemName = "@SYSTEM";

		private static readonly char[] InvalidChars = "'@\"".ToCharArray();

		private User(string name, bool validate) {
			if (validate && !IsValidName(name))
				throw new ArgumentException($"User name {name} is invalid");

			Name = name;
		}

		/// <summary>
		/// Constructs a <see cref="User"/> object that is identified
		/// by the given name.
		/// </summary>
		/// <param name="name">The name of the user</param>
		/// <exception cref="ArgumentException">If the provided name
		/// is not valid.</exception>
		/// <seealso cref="IsValidName"/>
		public User(string name)
			: this(name, true) { 
		}

		/// <summary>
		/// Gets the name of the user.
		/// </summary>
		public string Name { get; }

		/// <summary>
		/// Gets a boolean value indicating if this user is the
		/// special system user.
		/// </summary>
		/// <seealso cref="System"/>
		public bool IsSystem => String.Equals(Name, SystemName, StringComparison.OrdinalIgnoreCase);

		/// <summary>
		/// The special system user that has full access to any resource of the system
		/// </summary>
		public static User System = new User(SystemName, false);

		/// <summary>
		/// Safely asserts if a given name is valid for a user
		/// </summary>
		/// <param name="name">The user name to validate.</param>
		/// <returns>
		/// Returns a boolean value indicating if a given name
		/// is valid for a user.
		/// </returns>
		public static bool IsValidName(string name) {
			if (String.IsNullOrWhiteSpace(name))
				return false;

			return name.IndexOfAny(InvalidChars) < 0;
		}
	}
}