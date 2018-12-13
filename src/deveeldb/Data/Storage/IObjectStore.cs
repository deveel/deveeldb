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

namespace Deveel.Data.Storage {
	/// <summary>
	/// A storage that provides access and manages large objects for
	/// low-level data manipulation
	/// </summary>
	public interface IObjectStore : IDisposable {
		/// <summary>
		/// Creates an instance of a large object that has a given size
		/// </summary>
		/// <param name="maxSize">The maximum size (in bytes) of the object to create</param>
		/// <param name="compressed">Indicates if the data of the object must be
		/// compressed or not</param>
		/// <returns>
		/// Returns an instance of the large object created
		/// </returns>
		ILargeObject CreateObject(long maxSize, bool compressed);

		/// <summary>
		/// Gets a specific large object that is managed within the store
		/// </summary>
		/// <param name="objId">The unique identifier of the large object</param>
		/// <returns>
		/// Returns an instance of <see cref="ILargeObject"/> representing the large
		/// object identified
		/// </returns>
		ILargeObject GetObject(ObjectId objId);
	}
}