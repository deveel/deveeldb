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

namespace Deveel.Data {
	/// <summary>
	/// The possible type of a <see cref="IRef"/> object.
	/// </summary>
	/// <remarks>
	/// This enumeration includes the member <see cref="Compressed"/>
	/// that can be used to mark other kind of references as
	/// compressed.
	/// </remarks>
	[Flags]
	public enum ReferenceType : byte {
		/// <summary>
		/// This kind of reference handles binary data.
		/// </summary>
		Binary = 2,

		/// <summary>
		/// This kind of reference manages text data in ASCII format.
		/// </summary>
		AsciiText = 3,

		/// <summary>
		/// This kind of reference manages text data in UTF-16 format.
		/// </summary>
		UnicodeText = 4,

		/// <summary>
		/// A flag that marks a reference as compressed.
		/// </summary>
		Compressed = 0x010
	}
}