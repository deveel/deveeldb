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

namespace Deveel.Data.Store {
	/// <summary>
	/// The type of object reference that is possible
	/// to establish in a store.
	/// </summary>
	/// <seealso cref="IObjectRef.Type"/>
	/// <seealso cref="IObjectStore.GetObject"/>
	public enum ObjectType : byte {
		/// <summary>
		/// The object is a raw binary
		/// </summary>
		Binary = 1,

		/// <summary>
		/// The object is an ASCII 1-byte string type.
		/// </summary>
		AsciiString = 2,

		/// <summary>
		/// The object is an UNICODE 2-byte string type.
		/// </summary>
		UnicodeString = 3
	}
}