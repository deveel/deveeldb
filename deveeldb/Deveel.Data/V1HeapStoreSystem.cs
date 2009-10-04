//  
//  V1HeapStoreSystem.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Store;

namespace Deveel.Data {
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
	}
}