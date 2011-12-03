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

namespace Deveel.Data.Client {
	/// <summary>
	/// Defines the style used in a command to define the kind of 
	/// parameters permitted.
	/// </summary>
	public enum ParameterStyle {
		/// <summary>
		/// A marker parameter is represented by a question mark (<c>?</c>) 
		/// in the text of the command.
		/// </summary>
		Marker = 1,

		/// <summary>
		/// When using this style, parameters are represented by names prefixed
		/// by a <c>@</c> symbol.
		/// </summary>
		Named = 2
	}
}