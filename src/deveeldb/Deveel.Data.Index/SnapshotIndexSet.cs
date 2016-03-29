// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Index {
	internal class SnapshotIndexSet : IIndexSet {
		private readonly IndexSetStore indexSetStore;
		private List<StoreIndex> indexes;

		private bool disposed;

		private static readonly StoreIndex[] EmptyIndex = new StoreIndex[0];

		public SnapshotIndexSet(IndexSetStore indexSetStore, IndexBlock[] blocks) {
			this.indexSetStore = indexSetStore;
			IndexBlocks = blocks;

			// Not disposed.
			disposed = false;

		}

		~SnapshotIndexSet() {
			Dispose(false);
		}

		public IEnumerable<StoreIndex> AllIndices {
			get {
				if (indexes == null)
					return EmptyIndex;

				return indexes.ToArray();
			}
		}

		public IndexBlock[] IndexBlocks { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					try {
						if (indexes != null) {
							foreach (var index in indexes) {
								index.Dispose();
							}
						}

						// Release reference to the index_blocks;
						foreach (var block in IndexBlocks) {
							block.RemoveReference();
						}
					} catch (Exception) {
					}
				}

				indexes = null;
				IndexBlocks = null;
				disposed = true;
			}
		}


		public IIndex GetIndex(int offset) {
			// Create if not exist.
			if (indexes == null) {
				indexes = new List<StoreIndex>();
			} else {
				// If this list has already been created, return it
				foreach (var index in indexes) {
					if (index.IndexNumber == offset)
						return index;
				}
			}

			try {
				var index = (StoreIndex) IndexBlocks[offset].CreateIndex();
				indexes.Add(index);
				return index;
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message, e);
			}
		}
	}
}