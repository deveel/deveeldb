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

namespace Deveel.Data.Store {
	/// <summary>
	/// Enumerates the kind of storage possible
	/// for a <see cref="IStoreSystem"/>.
	/// </summary>
	public enum StorageType {
		/// <summary>
		/// The data are stored and retrieved within the
		/// underlying file-system.
		/// </summary>
		File,

		/// <summary>
		/// Storage medium is the random-access memory
		/// of the machine.
		/// </summary>
		Memory,

		/// <summary>
		/// A storage that spans through a network.
		/// </summary>
		Network
	}
}