//  
//  ICache.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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