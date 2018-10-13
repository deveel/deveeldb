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

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the meta information regarding a database object
	/// </summary>
	/// <remarks>
	/// <para>
	/// The information defined by implementations of <see cref="IDbObjectInfo"/>
	/// shape the behavior of the object they refer to.
	/// </para>
	/// <para>
	/// Object information can be used to create or alter objects (when supported),
	/// through the <c>CREATE</c> or <c>ALTER</c> statements.
	/// </para>
	/// <para>
	/// A database object must have a unique name, independently from its type:
	/// there cannot be two objects with the same name in a database (eg. a database
	/// cannot contain a stored procedure named <c>APP.obj1</c> and a table named <c>APP.obj1</c>). 
	/// </para>
	/// </remarks>
	/// <seealso cref="IDbObject"/>
	public interface IDbObjectInfo {
		/// <summary>
		/// Gets the specific type of the database object.
		/// </summary>
		DbObjectType ObjectType { get; }

		/// <summary>
		/// Gets the full name of the object that is unique within
		/// a database context.
		/// </summary>
		ObjectName FullName { get; }
	}
}
