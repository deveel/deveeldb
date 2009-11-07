//  
//  GrantObject.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An enumeration of the object of a grant operation.
	/// </summary>
	public enum GrantObject {
		/// <summary>
		/// Represents a TABLE object to grant privs over for the user.
		/// </summary>
		Table = 1,

		/// <summary>
		/// Represents a DOMAIN object to grant privs over for the user.
		/// </summary>
		Domain = 2,

		///<summary>
		/// Represents a STORED PROCEDURE object to grant privs over for this user.
		///</summary>
		StoredProcedure = 16,

		/// <summary>
		/// Represents a TRIGGER object to grant privs over for this user.
		/// </summary>
		Trigger = 17,

		/// <summary>
		/// Represents a custom SEQUENCE GENERATOR object to grant privs over.
		/// </summary>
		SequenceGenerator = 18,


		/// <summary>
		/// Represents a SCHEMA object to grant privs over for the user.
		/// </summary>
		Schema = 65,

		/// <summary>
		/// Represents a CATALOG object to grant privs over for this user.
		/// </summary>
		Catalog = 66
	}
}