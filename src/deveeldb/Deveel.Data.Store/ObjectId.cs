// 
//  Copyright 2010-2015 Deveel
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

namespace Deveel.Data.Store {
	/// <summary>
	/// A unique identifier of an object within a database system, 
	/// that is composed by a reference to the store it belongs to
	/// and the address of the object itself within the store.
	/// </summary>
	/// <remarks>
	/// It could happen that an object identifier is replicated
	/// by multiple stores, but the distinction is made by
	/// store: there cannot be two objects in the same store
	/// having the same identifier.
	/// </remarks>
	public struct ObjectId : IEquatable<ObjectId> {
		/// <summary>
		/// Constructs the <see cref="ObjectId"/> with the
		/// given references to the store and the address.
		/// </summary>
		/// <param name="storeId">The id of the store where the object belongs</param>
		/// <param name="id">The unique identifier of the object within the store.</param>
		public ObjectId(int storeId, long id)
			: this() {
			if (storeId < 0)
				throw new ArgumentOutOfRangeException("storeId");
			if (id < 0)
				throw new ArgumentOutOfRangeException("id");

			StoreId = storeId;
			Id = id;
		}

		/// <summary>
		/// Gets the unique identifier of the store that contains the object.
		/// </summary>
		/// <remarks>
		/// This is a unique value on a global level: it cannot happen that
		/// two stores have the same identifier in the same database system.
		/// </remarks>
		public int StoreId { get; private set; }

		/// <summary>
		/// Gets the unique identifier of the object within the containing store.
		/// </summary>
		/// <remarks>
		/// This is a unique value on a store level: in the same store, it cannot
		/// happen to find two objects having the same identifier.
		/// </remarks>
		public long Id { get; private set; }

		/// <inheritdoc/>
		public override bool Equals(object obj) {
			return Equals((ObjectId)obj);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return unchecked ((int)(StoreId*47 ^ Id));
		}

		/// <inheritdoc/>
		public bool Equals(ObjectId other) {
			return StoreId == other.StoreId &&
			       Id == other.Id;
		}

		/// <inheritdoc/>
		public override string ToString() {
			return String.Format("0x{0:X}:{1:X}", StoreId, Id);
		}
	}
}