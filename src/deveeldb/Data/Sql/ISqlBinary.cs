// 
//  Copyright 2010-2017 Deveel
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
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the required contract of a SQL <c>BINARY</c> object
	/// </summary>
	public interface ISqlBinary : ISqlValue, IEnumerable<byte> {
		/// <summary>
		/// Gets the raw length of the binary object.
		/// </summary>
		long Length { get; }

		/// <summary>
		/// Gets an object used to read the contents of the binary
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="Stream"/> that is a read-only
		/// interface for accessing the contents handled by this binary.
		/// </returns>
		Stream GetInput();
	}
}