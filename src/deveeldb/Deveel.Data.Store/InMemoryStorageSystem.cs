// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Store {
	public sealed class InMemoryStorageSystem : IStoreSystem {
		private Dictionary<string, InMemoryStore> nameStoreMap;

		public const int DefaultStoreSize = 1024;

		public InMemoryStorageSystem() {
			nameStoreMap = new Dictionary<string, InMemoryStore>();
		}

		~InMemoryStorageSystem() {
			Dispose(false);
		}
 
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (nameStoreMap != null) {
					lock (this) {
						foreach (var store in nameStoreMap.Values) {
							if (store != null)
								store.Dispose();
						}

						nameStoreMap.Clear();
					}
				}
			}

			lock (this) {
				nameStoreMap = null;
			}
		}

		public bool StoreExists(string name) {
			lock (this) {
				return nameStoreMap.ContainsKey(name);
			}
		}

		IStore IStoreSystem.CreateStore(string name) {
			return CreateStore(name, DefaultStoreSize);
		}

		public InMemoryStore CreateStore(string name) {
			return CreateStore(name, DefaultStoreSize);
		}

		public InMemoryStore CreateStore(string name, int hashSize) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			lock (this) {
				if (StoreExists(name))
					throw new ArgumentException(String.Format("The store {0} already exists.", name));

				var store = new InMemoryStore(name, hashSize);
				nameStoreMap[name] = store;
				return store;
			}
		}

		public InMemoryStore OpenStore(string name) {
			lock (this) {
				InMemoryStore store;
				if (!nameStoreMap.TryGetValue(name, out store))
					throw new InvalidOperationException(String.Format("Store {0} does not exist.", name));

				return store;
			}
		}

		IStore IStoreSystem.OpenStore(string name) {
			return OpenStore(name);
		}

		bool IStoreSystem.CloseStore(IStore store) {
			return CloseStore((InMemoryStore) store);
		}

		public bool CloseStore(InMemoryStore store) {
			if (!StoreExists(store.Name))
				throw new InvalidOperationException(String.Format("Store {0} does not exist", store.Name));

			return true;
		}

		bool IStoreSystem.DeleteStore(IStore store) {
			return DeleteStore((InMemoryStore) store);
		}

		public bool DeleteStore(InMemoryStore store) {
			if (store == null) 
				throw new ArgumentNullException("store");

			lock (this) {
				InMemoryStore removed;
				if (!nameStoreMap.TryGetValue(store.Name, out removed))
					return false;

				if (removed != null)
					removed.Dispose();

				return true;
			}
		}

		public void SetCheckPoint() {
			// Check point logging not necessary with memory store
		}

		public void Lock(string lockName) {
		}

		public void Unlock(string lockName) {
		}
	}
}