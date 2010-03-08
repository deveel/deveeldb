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
	/// The types of data handled by the database system.
	/// </summary>
	/// <remarks>
	/// This enumeration specifies all the data types
	/// that the system can manage: SQL specifications
	/// define a wider range of types (eg. <c>VARCHAR</c>,
	/// <c>INTEGER</c>, <c>TIME</c>, etc.), while the
	/// system encapsulates each type into a domain generic
	/// type.
	/// </remarks>
	public enum DbType {
		/// <summary>
		/// An unknown database type.
		/// </summary>
		Unknown = -1,

		/// <summary>
		/// A generic medium-string type.
		/// </summary>
		/// <remarks>
		/// This type can handle strings of smaller size: 
		/// for greater sizes it will be necessary to use
		/// <see cref="Blob"/> type.
		/// </remarks>
		String = 1,

		///<summary>
		/// A generic numeric type that handles all the
		/// numeric values stored into the system.
		///</summary>
		Numeric = 2,

		/// <summary>
		/// The time type that handles dates and times within
		/// the database system.
		/// </summary>
		Time = 3,

		///<summary>
		///</summary>
		[Obsolete("Use BLOB instead.")]
		Binary = 4,

		///<summary>
		/// A type that handles boolean types (either <c>true</c>
		/// or <c>false</c>).
		///</summary>
		Boolean = 5,

		///<summary>
		/// The type used to handle binary data in a database.
		///</summary>
		/// <remarks>
		/// This is also used to store CLOB data.
		/// </remarks>
		Blob = 6,

		///<summary>
		///</summary>
		Object = 7,

		///<summary>
		/// An extended numeric type that handles neg and positive 
		/// infinity and NaN.
		///</summary>
		NumericExtended = 8

	}
}