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
using System.Collections;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of <see cref="IStoreSystem"/> that stores all persistent 
	/// data on the heap using <see cref="HeapStore"/> objects.
	/// </summary>
	class V1HeapStoreSystem : IStoreSystem {
		/// <summary>
		/// A mapping from name to Store object for this heap store system.
		/// </summary>
		private readonly Hashtable name_store_map;
		/// <summary>
		/// A mapping from Store object to name.
		/// </summary>
		private readonly Hashtable store_name_map;

		internal V1HeapStoreSystem() {
			name_store_map = new Hashtable();
			store_name_map = new Hashtable();
		}

		public StorageType StorageType {
			get { return StorageType.Memory; }
		}

		/// <inheritdoc/>
		public void Init(SystemContext context) {
		}

		/// <inheritdoc/>
		public bool StoreExists(String name) {
			return (name_store_map[name] != null);
		}

		/// <inheritdoc/>
		public IStore CreateStore(String name) {
			if (!StoreExists(name)) {
				HeapStore store = new HeapStore();
				name_store_map.Add(name, store);
				store_name_map.Add(store, name);
				return store;
			}
			
			throw new Exception("Store exists: " + name);
		}

		/// <inheritdoc/>
		public IStore OpenStore(String name) {
			HeapStore store = (HeapStore)name_store_map[name];
			if (store == null) {
				throw new Exception("Store does not exist: " + name);
			}
			return store;
		}

		/// <inheritdoc/>
		public bool CloseStore(IStore store) {
			if (store_name_map[store] == null) {
				throw new Exception("Store does not exist.");
			}
			return true;
		}

		/// <inheritdoc/>
		public bool DeleteStore(IStore store) {
			String name = (String)store_name_map[store];
			store_name_map.Remove(store);
			name_store_map.Remove(name);
			return true;
		}

		/// <inheritdoc/>
		public void SetCheckPoint() {
			// Check point logging not necessary with heap store
		}

		/// <inheritdoc/>
		public void Lock(String lock_name) {
			// Not required because heap memory is not a shared resource that can be
			// accessed by multiple JVMs
		}

		/// <inheritdoc/>
		public void Unlock(String lock_name) {
			// Not required because heap memory is not a shared resource that can be
			// accessed by multiple JVMs
		}

		public void Dispose() {
			store_name_map.Clear();
			name_store_map.Clear();
		}
	}
}