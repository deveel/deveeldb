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
	/// Represents a database object, such as a table, a trigger,
	/// a type or a column.
	/// </summary>
	/// <remarks>
	/// Database objects are defined by a set of information
	/// that shape their behavior within the system (<see cref="IDbObjectInfo"/>).
	/// </remarks>
	public interface IDbObject {
		/// <summary>
		/// Gets the database object information.
		/// </summary>
		IDbObjectInfo ObjectInfo { get; }
	}
}