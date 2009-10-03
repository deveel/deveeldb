// 
//  StatementCache.cs
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

using Deveel.Diagnostics;
using Deveel.Data.Util;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A cache that maintains a serialized set of <see cref="StatementTree"/> 
	/// objects that can be deserialized on demand.
	/// </summary>
	/// <remarks>
	/// The purpose of this cache is to improve the performance of queries 
	/// that are run repeatedly (for example, multiple INSERT statements).
	/// </remarks>
	public sealed class StatementCache {
		/// <summary>
		/// The DatabaseSystem of this cache.
		/// </summary>
		private readonly DatabaseSystem system;

		/// <summary>
		/// The internal cache representation.
		/// </summary>
		private readonly Cache cache;

		///<summary>
		///</summary>
		///<param name="system"></param>
		///<param name="hash_size"></param>
		///<param name="max_size"></param>
		///<param name="clean_percentage"></param>
		public StatementCache(DatabaseSystem system,
		                      int hash_size, int max_size, int clean_percentage) {
			this.system = system;
			cache = new Cache(hash_size, max_size, clean_percentage);
		}

		/// <summary>
		/// Returns a IDebugLogger object we can use to log debug messages.
		/// </summary>
		/*
		TODO:
		public IDebugLogger Debug {
			get { return system.Debug; }
		}
		*/

		/// <summary>
		/// Puts a new query string/StatementTree into the cache.
		/// </summary>
		/// <param name="query_string"></param>
		/// <param name="statement_tree"></param>
		public void Set(String query_string, StatementTree statement_tree) {
			lock (this) {
				query_string = query_string.Trim();
				// Is this query string already in the cache?
				if (cache.Get(query_string) == null) {
					try {
						Object cloned_tree = statement_tree.Clone();
						cache.Set(query_string, cloned_tree);
					} catch (Exception e) {
						Debug.WriteException(e);
						throw new ApplicationException("Unable to clone statement tree: " + e.Message);
					}
				}
			}
		}

		///<summary>
		/// Gets a StatementTree for the query string if it is stored in the cache.
		///</summary>
		///<param name="query_string"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public StatementTree Get(String query_string) {
			lock (this) {
				query_string = query_string.Trim();
				Object ob = cache.Get(query_string);
				if (ob != null) {
					try {
						//        Console.Out.WriteLine("CACHE HIT!");
						// We found a cached version of this query so deserialize and return
						// it.
						StatementTree cloned_tree = (StatementTree)ob;
						return (StatementTree)cloned_tree.Clone();
					} catch (Exception e) {
						Debug.WriteException(e);
						throw new ApplicationException("Unable to clone statement tree: " + e.Message);
					}
				}
				// Not found so return null
				return null;
			}
		}

	}
}