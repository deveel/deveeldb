using System;
using System.Collections.Generic;
using System.IO;

namespace Deveel.Data.Index {
	internal class SnapshotIndexSet : IIndexSet {
		private readonly IndexSetStore indexSetStore;
		private IndexBlock[] blocks;
		private List<StoreIndex> indexes;

		private bool disposed;

		private static readonly StoreIndex[] EmptyIndex = new StoreIndex[0];

		public SnapshotIndexSet(IndexSetStore indexSetStore, IndexBlock[] blocks) {
			this.indexSetStore = indexSetStore;
			this.blocks = blocks;

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

		public IndexBlock[] IndexBlocks {
			get { return blocks; }
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				try {
					if (!disposed) {
						DoDispose();
					}
				} catch (Exception e) {
				}
			}
		}

		private void DoDispose() {
			if (!disposed) {
				if (indexes != null) {
					foreach (var index in indexes) {
						index.Dispose();
					}
					indexes = null;
				}

				// Release reference to the index_blocks;
				foreach (var block in blocks) {
					block.RemoveReference();
				}

				blocks = null;
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
				var index = (StoreIndex) blocks[offset].CreateIndex();
				indexes.Add(index);
				return index;
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message, e);
			}
		}
	}
}