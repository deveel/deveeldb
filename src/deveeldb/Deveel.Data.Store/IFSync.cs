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

namespace Deveel.Data.Store {
	/// <summary>
	/// An interface providing a contract for the synchronization
	/// of a <see cref="FileStream"/> with the underlying file-system.
	/// </summary>
	public interface IFSync {
		/// <summary>
		/// Synchronizes the latest modifications on the given
		/// <see cref="FileStream"/> to the underlying file-system.
		/// </summary>
		/// <param name="stream">The <see cref="FileStream"/> to synchronize
		/// with the underlying file-system.</param>
		/// <exception cref="SyncFailedException">
		/// If the synchronization operation fails for any reason.
		/// </exception>
		void Sync(FileStream stream);
	}
}