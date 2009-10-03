// 
//  HashMapList.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

namespace Deveel.Data.Util {
	///<summary>
	/// A <see cref="Hashtable"/> that maps from a source to a list of items 
	/// for that source.
	///</summary>
	/// <remarks>
	/// This is useful as a searching mechanism where the list of searched 
	/// items are catagorised in the mapped list.
	/// </remarks>
	public class HashMapList {

		private static readonly IList EMPTY_LIST = new Object[0];

		private Hashtable map;

		/// <summary>
		/// Constructs the map.
		/// </summary>
		public HashMapList() {
			map = new Hashtable();
		}

		/// <summary>
		/// Puts a value into the map list.
		/// </summary>
		/// <param name="key"></param>
		/// <param name="val"></param>
		public void Add(Object key, Object val) {
			ArrayList list = (ArrayList)map[key];
			if (list == null) {
				list = new ArrayList();
			}
			list.Add(val);
			map[key] = list;
		}

		///<summary>
		/// Returns the list of values that are in the map under this key.
		///</summary>
		///<param name="key"></param>
		/// <remarks>
		/// Returns an empty list if no key map found.
		/// </remarks>
		public IList this[Object key] {
			get {
				ArrayList list = (ArrayList) map[key];
				if (list != null) {
					return list;
				}
				return EMPTY_LIST;
			}
		}

		///<summary>
		/// Removes the given value from the list with the given key.
		///</summary>
		///<param name="key"></param>
		///<param name="val"></param>
		///<returns></returns>
		public bool Remove(Object key, Object val) {
			ArrayList list = (ArrayList)map[key];
			if (list == null) {
				return false;
			}
			int index = list.IndexOf(val);
			if (index != -1)
				list.RemoveAt(index);
			if (list.Count == 0) {
				map.Remove(key);
			}
			return index != -1;
		}

		///<summary>
		/// Clears the all the values for the given key.
		///</summary>
		///<param name="key"></param>
		///<returns>
		/// Returns the <see cref="IList"/> of items that were 
		/// stored under this key.
		/// </returns>
		public IList Clear(Object key) {
			ArrayList list = (ArrayList)map[key];
			map.Remove(key);
			if (list == null) {
				return new ArrayList();
			}
			return list;
		}

		public ICollection Keys {
			get { return map.Keys; }
		}

		/// <summary>
		/// Returns true if the map contains the key.
		/// </summary>
		/// <param name="key"></param>
		/// <returns></returns>
		public bool ContainsKey(Object key) {
			return map.ContainsKey(key);
		}
	}
}