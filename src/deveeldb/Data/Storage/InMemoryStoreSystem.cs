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
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

using Deveel.Data.Configurations;

namespace Deveel.Data.Storage {
	public class InMemoryStoreSystem : IStoreSystem {
		private Dictionary<string, InMemoryStore> nameStoreMap;

		public InMemoryStoreSystem() {
			nameStoreMap = new Dictionary<string, InMemoryStore>();
		}

		public string SystemId => "memory";

		public bool StoreExists(string name) {
			lock (this) {
				return nameStoreMap.ContainsKey(name);
			}
		}

		IStore IStoreSystem.CreateStore(string name, IConfiguration configuration) {
			return CreateStore(name, configuration);
		}

		public InMemoryStore CreateStore(string name, IConfiguration configuration) {
			var hashSize = configuration.GetInt32("hashSize", 1024);

			lock (this) {
				if (nameStoreMap.ContainsKey(name))
					throw new IOException($"A store named '{name}' already in the systme");

				var store = new InMemoryStore(name, hashSize);
				nameStoreMap[name] = store;
				return store;
			}
		}

		IStore IStoreSystem.OpenStore(string name, IConfiguration configuration) {
			return OpenStore(name, configuration);
		}

		public InMemoryStore OpenStore(string name, IConfiguration configuration) {
			lock (this) {
				InMemoryStore store;
				if (!nameStoreMap.TryGetValue(name, out store))
					throw new IOException($"No store with name '{name}' was found in the system");

				return store;
			}
		}

		bool IStoreSystem.CloseStore(IStore store) {
			return CloseStore((InMemoryStore) store);
		}

		public bool CloseStore(InMemoryStore store) {
			lock (this) {
				if (!nameStoreMap.ContainsKey(store.Name))
					throw new IOException($"The store '{store.Name}' was not found in the system");

				return true;
			}
		}

		bool IStoreSystem.DeleteStore(IStore store) {
			return DeleteStore((InMemoryStore) store);
		}

		public bool DeleteStore(InMemoryStore store) {
			lock (this) {
				try {
					return nameStoreMap.Remove(store.Name);
				} finally {
					store.Dispose();
				}
			}
		}

		public void SetCheckPoint() {
		}

		public void Lock(string lockKey) {
		}

		public void Unlock(string lockKey) {
		}

		private void Clean() {
			lock (this) {
				if (nameStoreMap != null) {
					foreach (var store in nameStoreMap.Values) {
						if (store != null)
							store.Dispose();
					}

					nameStoreMap.Clear();
				}
			}
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				Clean();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}