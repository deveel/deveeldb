// 
//  DbConfig.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

using Deveel.Data.Control;

namespace Deveel.Data {
	/// <summary>
	///  An basic implementation of <see cref="IDbConfig"/>.
	/// </summary>
	public class DbConfig : IDbConfig {
		/// <summary>
		/// The current base path of the database configuration.
		/// </summary>
		private readonly string current_path;

		/// <summary>
		/// The Hashtable mapping from configuration key to value for the key.
		/// </summary>
		private Hashtable key_map;

		/// <summary>
		/// Constructs the <see cref="IDbConfig"/>.
		/// </summary>
		/// <param name="current_path"></param>
		public DbConfig(string current_path) {
			this.current_path = current_path;
			this.key_map = new Hashtable();
		}

		/// <summary>
		/// Returns the default value for the configuration property with the 
		/// given key.
		/// </summary>
		/// <param name="property_key"></param>
		/// <returns></returns>
		protected virtual String GetDefaultValue(String property_key) {
			// This abstract implementation returns null for all default keys.
			return null;
		}

		///<summary>
		/// Sets the configuration value for the key property key.
		///</summary>
		///<param name="property_key"></param>
		///<param name="val"></param>
		public virtual void SetValue(String property_key, String val) {
			key_map[property_key] = val;
		}

		// ---------- Implemented from IDbConfig ----------

		public string CurrentPath {
			get { return current_path; }
		}

		public String GetValue(String property_key) {
			// If the key is in the map, return it here
			String val = (String)key_map[property_key];
			if (val == null) {
				return GetDefaultValue(property_key);
			}
			return val;
		}

		public IDbConfig ImmutableCopy() {
			DbConfig immutable_copy = new DbConfig(current_path);
			immutable_copy.key_map = (Hashtable)key_map.Clone();
			return immutable_copy;
		}
	}
}