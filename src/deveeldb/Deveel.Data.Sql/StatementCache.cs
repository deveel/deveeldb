// 
//  Copyright 2010-2014 Deveel
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
using System.Collections.Generic;

using Deveel.Data.Caching;
using Deveel.Data.DbSystem;
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
		private readonly DatabaseContext context;

		/// <summary>
		/// The internal cache representation.
		/// </summary>
		private readonly Cache cache;

		///<summary>
		///</summary>
		///<param name="context"></param>
		///<param name="hashSize"></param>
		///<param name="maxSize"></param>
		///<param name="cleanPercentage"></param>
		public StatementCache(DatabaseContext context, int hashSize, int maxSize, int cleanPercentage) {
			this.context = context;
			cache = new MemoryCache(hashSize, maxSize, cleanPercentage);
		}

		private ILogger Logger {
			get { return context.Logger; }
		}

		/// <summary>
		/// Puts a new command string/List{StatementTree} into the cache.
		/// </summary>
		/// <param name="queryString"></param>
		/// <param name="statementTreeList"></param>
		public void Set(string queryString, IList<StatementTree> statementTreeList) {
			lock (this) {
				queryString = queryString.Trim();
				// Is this command string already in the cache?
				if (cache.Get(queryString) == null) {
					try {
						List<StatementTree> clonedList = new List<StatementTree>(statementTreeList.Count);
						foreach (StatementTree statementTree in statementTreeList) {
							StatementTree cloned_tree = (StatementTree) statementTree.Clone();
							clonedList.Add(cloned_tree);
						}
						cache.Set(queryString, clonedList);
					} catch (Exception e) {
						Logger.Error(this, e);
						throw new ApplicationException("Unable to clone statement tree: " + e.Message);
					}
				}
			}
		}

		///<summary>
		/// Gets a StatementTree for the command string if it is stored in the cache.
		///</summary>
		///<param name="queryString"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public IList<StatementTree> Get(string queryString) {
			lock (this) {
				queryString = queryString.Trim();
				object ob = cache.Get(queryString);
				if (ob != null) {
					try {
						// We found a cached version of this command so deserialize and return it.
						List<StatementTree> clonedList = new List<StatementTree>();
						IList<StatementTree> statementTreeList = (IList<StatementTree>)ob;
						foreach (StatementTree statementTree in statementTreeList) {
							clonedList.Add((StatementTree)statementTree.Clone());
						}
						return clonedList;
					} catch (Exception e) {
						Logger.Error(this, e);
						throw new ApplicationException("Unable to clone statement tree: " + e.Message);
					}
				}
				// Not found so return null
				return null;
			}
		}

	}
}