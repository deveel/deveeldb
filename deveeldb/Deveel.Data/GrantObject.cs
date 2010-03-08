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