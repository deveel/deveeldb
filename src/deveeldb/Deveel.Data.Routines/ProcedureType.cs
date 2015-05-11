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

namespace Deveel.Data.Routines {
	/// <summary>
	/// The form of a database stored <c>PROCEDURE</c>.
	/// </summary>
	public enum ProcedureType {
		/// <summary>
		/// A procedure that requires no state to be executed.
		/// </summary>
		Static = 1,

		/// <summary>
		/// A stored procedure defined by a user.
		/// </summary>
		UserDefined = 2,

		/// <summary>
		/// An external program that is only referenced by the
		/// procedure information.
		/// </summary>
		External = 3
	}
}
