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

namespace Deveel.Data.Security {
	/// <summary>
	/// Contains the information about the access control to a user
	/// from a defined connection protocol to the database.
	/// </summary>
	public sealed class UserProtocolAccess {
		/// <summary>
		/// Identifies an address as matching all possible ranges for
		/// the connection protocol.
		/// </summary>
		public const string AnyAddress = "%";

		/// <summary>
		/// Constructs a user access control that matches all the addresses
		/// for the given protocol.
		/// </summary>
		/// <param name="userName">The name of the user holding the control.</param>
		/// <param name="protocol">The name of the protocol to control access to
		/// the database for a given user.</param>
		/// <param name="privilege">The kind of access to provide to the user for the 
		/// protocol given.</param>
		/// <seealso cref="AnyAddress"/>
		public UserProtocolAccess(string userName, string protocol, AccessPrivilege privilege) 
			: this(userName, protocol, AnyAddress, privilege) {
		}
		/// <summary>
		/// Constructs a user access control that matches the address for the 
		/// given protocol.
		/// </summary>
		/// <param name="userName">The name of the user holding the control.</param>
		/// <param name="protocol">The name of the protocol to control access to
		/// the database for a given user.</param>
		/// <param name="address">The specific address to which to apply this control.</param>
		/// <param name="privilege">The kind of access to provide to the user for the 
		/// protocol given.</param>
		/// <seealso cref="AnyAddress"/>
		public UserProtocolAccess(string userName, string protocol, string address, AccessPrivilege privilege) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (String.IsNullOrEmpty(protocol))
				throw new ArgumentNullException("protocol");

			UserName = userName;
			Protocol = protocol;
			Address = address;
			Privilege = privilege;
		}

		/// <summary>
		/// Gets the name of the user that holds the control.
		/// </summary>
		public string UserName { get; private set; }

		/// <summary>
		/// Gets the connection protocol on which to apply the access control.
		/// </summary>
		public string Protocol { get; private set; }

		/// <summary>
		/// Gets the address on which to apply the access control for the user.
		/// </summary>
		public string Address { get; private set; }

		/// <summary>
		/// Gets the kind of access privilege for the access control of the user.
		/// </summary>
		public AccessPrivilege Privilege { get; private set; }

		/// <summary>
		/// Gets a boolean value that indicates if this access control will be
		/// applied to all ranges of address of a protocol.
		/// </summary>
		/// <seealso cref="AnyAddress"/>
		public bool IsForAnyAddress {
			get { return Address.Equals(AnyAddress); }
		}
	}
}
