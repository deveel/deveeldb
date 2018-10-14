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

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The access type to a lockable object in a database
	/// </summary>
	[Flags]
	public enum AccessType {
		/// <summary>
		/// The resource is accessed to be read
		/// </summary>
		Read = 1,

		/// <summary>
		/// The resource is accessed to be written
		/// </summary>
		Write = 2,

		/// <summary>
		/// The resource is accessed to be read and written
		/// </summary>
		ReadWrite = Read | Write
	}
}