// 
//  Copyright 2010-2014 Deveel
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

namespace Deveel.Data.Threading {
	/// <summary>
	/// An enumeration which defines the access type of a <see cref="Lock"/>.
	/// </summary>
	public enum AccessType {
		///<summary>
		/// The <see cref="Lock"/> is a <c>READ</c> lock.
		///</summary>
		Read = 0,

		/// <summary>
		/// The <see cref="Lock"/> is a <c>WRITE</c> lock.
		/// </summary>
		Write = 1
	}
}