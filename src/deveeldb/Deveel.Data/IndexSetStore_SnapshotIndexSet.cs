// 
//  Copyright 2011 Deveel
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
using System.IO;

namespace Deveel.Data {
	sealed partial class IndexSetStore {
		/// <summary>
		/// The implementation of IIndexSet which represents a mutable snapshot of 
		/// the indices stored in this set.
		/// </summary>
		private class SnapshotIndexSet : IIndexSet {
			private readonly IndexSetStore store;

			/// <summary>
			/// The list of IndexBlock object that represent the view of the index set
			/// when the view was created.
			/// </summary>
			private IndexBlock[] blocks;

			/// <summary>
			/// The list of <see cref="Index"/> objects that have been returned 
			/// via the <see cref="GetIndex"/> method.
			/// </summary>
			private List<IIndex> indexes;

			/// <summary>
			/// Set to true when this object is disposed.
			/// </summary>
			private bool disposed;


			public SnapshotIndexSet(IndexSetStore store, IndexBlock[] blocks) {
				this.store = store;
				this.blocks = blocks;

				// Not disposed.
				disposed = false;

			}

			~SnapshotIndexSet() {
				Dispose(false);
			}

			/// <summary>
			/// Returns all the indexes that have been created by calls to <see cref="GetIndex"/>.
			/// </summary>
			public IEnumerable<IIndex> AllIndices {
				get {
					if (indexes == null)
						return EmptyIndex;

					return indexes.ToArray();
				}
			}

			/// <summary>
			/// The list of IndexBlock object that represent the view of the index set
			/// when the view was created.
			/// </summary>
			public IndexBlock[] IndexBlocks {
				get { return blocks; }
			}

			// ---------- Implemented from IIndexSet ----------

			public IIndex GetIndex(int n) {
				// Create if not exist.
				if (indexes == null) {
					indexes = new List<IIndex>();
				} else {
					// If this list has already been created, return it
					foreach (Index index in indexes) {
						if (index.IndexNumber == n)
							return index;
					}
				}

				try {
					IIndex index = blocks[n].CreateIndex();
					indexes.Add(index);
					return index;
				} catch (IOException e) {
					store.system.Logger.Error(this, e);
					throw new Exception("IO Error: " + e.Message, e);
				}

			}

			private void Dispose() {
				if (!disposed) {
					if (indexes != null) {
						foreach (Index index in indexes) {
							index.Dispose();
						}
						indexes = null;
					}

					// Release reference to the index_blocks;
					foreach (IndexBlock block in blocks) {
						block.RemoveReference();
					}

					blocks = null;
					disposed = true;
				}
			}

			void IDisposable.Dispose() {
				GC.SuppressFinalize(this);
				Dispose(true);
			}

			private void Dispose(bool disposing) {
				if (disposing) {
					try {
						if (!disposed) {
							Dispose();
						}
					} catch (Exception e) {
						store.system.Logger.Error(this, "Finalize error: " + e.Message);
						store.system.Logger.Error(this, e);
					}
				}
			}
		} 
	}
}