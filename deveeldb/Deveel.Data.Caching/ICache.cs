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

namespace Deveel.Data.Caching {
	/// <summary>
	/// Provides a contract to access a caching system.
	/// </summary>
	public interface ICache : IDisposable {
		/// <summary>
		/// Gets an object for the given key from the underlying cache system.
		/// </summary>
		/// <param name="key">The key of the object to return.</param>
		/// <returns>
		/// Returns an <see cref="object"/> for the given <paramref name="key"/>,
		/// or <b>null</b> if the key was not found or the object stored is
		/// <b>null</b>.
		/// </returns>
		object Get(object key);

		/// <summary>
		/// Sets an object into the underlying caching system.
		/// </summary>
		/// <param name="key">The key used to identify the object to store.</param>
		/// <param name="value">The object value to store into the cache.</param>
		/// <returns>
		/// Returns <b>true</b> if the given value was newly added, or <b>false</b>
		/// if an object for the given key was previously set and the method only
		/// sets a new value.
		/// </returns>
		bool Set(object key, object value);

		/// <summary>
		/// Removes an object from the cache.
		/// </summary>
		/// <param name="key">The key referencing the object to remove from the cache.</param>
		/// <returns>
		/// Returns the value of the object removed from the cache, or <b>null</b>
		/// if the key was not found or the value stored was <b>null</b>.
		/// </returns>
		object Remove(object key);

		/// <summary>
		/// Clears all the cached items within the underlying system.
		/// </summary>
		void Clear();
	}
}