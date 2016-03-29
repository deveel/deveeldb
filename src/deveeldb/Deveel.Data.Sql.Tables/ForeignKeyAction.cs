// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// Enumerates the foreign key referential trigger actions.
	/// </summary>
	public enum ForeignKeyAction {
		/// <summary>
		/// No actions to be done on the foreign key trigger.
		/// </summary>
		NoAction = 0,

		Cascade = 1,

		/// <summary>
		/// Sets the value of the reference column to <c>null</c>.
		/// </summary>
		SetNull = 2,

		/// <summary>
		/// Sets the default value of the referenced column, if any was set.
		/// </summary>
		SetDefault = 3
	}
}