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

namespace Deveel.Data.Sql {
	/// <summary>
	/// The types of constraints that can be applied to a
	/// column at definition.
	/// </summary>
	[Flags]
	enum ColumnConstraints {
		/// <summary>
		/// None constraint for the column was set.
		/// </summary>
		None = 0x00,

		/// <summary>
		/// The column belongs to a <c>PRIMARY KEY</c> constraint
		/// within the containing table.
		/// </summary>
		PrimaryKey = 0x01,

		/// <summary>
		/// Indiciates that the value of a column must be <c>UNIQUE</c>
		/// for each row.
		/// </summary>
		Unique = 0x02,

		/// <summary>
		/// Constraints a column to contain only values that are
		/// <c>NOT NULL</c>.
		/// </summary>
		NotNull = 0x04,

		/// <summary>
		/// All the column constraints possible.
		/// </summary>
		All = PrimaryKey | Unique | NotNull
	}
}