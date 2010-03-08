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

using Deveel.Data.Caching;
using Deveel.Diagnostics;

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
		public StatementCache(DatabaseSystem system, int hash_size, int max_size, int clean_percentage) {
			this.system = system;
			cache = new MemoryCache(hash_size, max_size, clean_percentage);
		}

		private IDebugLogger Debug {
			get { return system.Debug; }
		}

		/// <summary>
		/// Puts a new command string/StatementTree into the cache.
		/// </summary>
		/// <param name="query_string"></param>
		/// <param name="statement_tree"></param>
		public void Set(String query_string, StatementTree statement_tree) {
			lock (this) {
				query_string = query_string.Trim();
				// Is this command string already in the cache?
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
		/// Gets a StatementTree for the command string if it is stored in the cache.
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
						// We found a cached version of this command so deserialize and return
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