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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// An interface that provides access to basic information about a BLOB 
	/// so that we may compare BLOBs implemented in different ways.
	/// </summary>
	public interface IBlobAccessor {
		/// <summary>
		/// Gets the size of the BLOB.
		/// </summary>
		int Length { get; }

		/// <summary>
		/// Gets a stream that allows us to read the contents of the blob 
		/// from start to finish.
		/// </summary>
		/// <remarks>
		/// This object should be wrapped in a <see cref="BufferedStream"/> if 
		/// <see cref="Stream.Read"/> type efficiency is required.
		/// </remarks>
		/// <returns>
		/// </returns>
		Stream GetInputStream();

	}
}