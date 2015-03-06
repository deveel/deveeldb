// 
//  Copyright 2010-2015 Deveel
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
using System.IO;

namespace Deveel.Data.Store {
	public sealed class FixedRecordList {
		private const int Magic = 0x087131AA;

		private readonly IStore store;
		private readonly int elementSize;

		private long headerAreaId;
		private IArea headerArea;
		private int blockCount;

		// Pointers to the blocks in the list block.
		private readonly long[] blockElements;
		private readonly IArea[] blockAreas;

		public FixedRecordList(IStore store, int elementSize) {
			this.store = store;
			this.elementSize = elementSize;
			blockElements = new long[64];
			blockAreas = new IArea[64];
		}

		public int BlockCount {
			get { return blockCount; }
		}

		public long NodeCount {
			get { return BlockFirstPosition(blockCount); }
		}

		private void UpdateListHeaderArea() {
			headerArea.Position = 4;
			headerArea.WriteInt4(blockCount);
			headerArea.Position = 16;
			for (int i = 0; i < blockCount; ++i) {
				headerArea.WriteInt8(blockElements[i]);
			}
			headerArea.Flush();
		}

		public long Create() {
			// Allocate space for the list header (8 + 8 + (64 * 8))
			IArea writer = store.CreateArea(528);
			headerAreaId = writer.Id;
			writer.WriteInt4(Magic);
			writer.Flush();

			headerArea = store.GetArea(headerAreaId);
			blockCount = 0;
			UpdateListHeaderArea();

			return headerAreaId;
		}

		public void Open(long listPointer) {
			headerAreaId = listPointer;
			headerArea = store.GetArea(headerAreaId);

			int magic = headerArea.ReadInt4(); // MAGIC
			if (magic != Magic)
				throw new IOException("Incorrect magic for list block. [magic=" + magic + "]");

			blockCount = headerArea.ReadInt4();
			headerArea.ReadInt8(); // Delete Chain Head
			for (int i = 0; i < blockCount; ++i) {
				long blockPointer = headerArea.ReadInt8();
				blockElements[i] = blockPointer;
				blockAreas[i] = store.GetArea(blockPointer);
			}
		}

		public long ReadDeleteHead() {
			headerArea.Position = 8;
			return headerArea.ReadInt8();
		}

		public void WriteDeleteHead(long value) {
			headerArea.Position = 8;
			headerArea.WriteInt8(value);
			headerArea.Flush();
		}

		public IArea GetRecord(long recordNumber) {
			// What block is this record in?
			int bit = 0;
			long work = recordNumber + 32;
			while (work != 0) {
				work = work >> 1;
				++bit;
			}

			long startOffset = (1 << (bit - 1)) - 32;
			int blockOffset = bit - 6;
			long recordOffset = recordNumber - startOffset;

			// Get the pointer to the block that contains this record status
			IArea blockArea = blockAreas[blockOffset];
			blockArea.Position = (int) (recordOffset*elementSize);
			return blockArea;
		}

		public long BlockNodeCount(int blockNumber) {
			return 32L << blockNumber;
		}

		public long BlockFirstPosition(int blockNumber) {
			// For example, this first node of block 0 is 0, the first node of block 1 is
			// 32, the first node of block 2 is 96, etc.
			long startIndex = 0;
			int i = blockNumber;
			long diff = 32;
			while (i > 0) {
				startIndex = startIndex + diff;
				diff = diff << 1;
				--i;
			}
			return startIndex;
		}

		public void IncreaseSize() {
			// The size of the block
			long sizeOfBlock = 32L << blockCount;

			// Allocate the new block in the store
			IArea writer = store.CreateArea(sizeOfBlock * elementSize);
			long blockId = writer.Id;
			writer.Flush();

			IArea blockArea = store.GetArea(blockId);
			// Update the block list
			blockElements[blockCount] = blockId;
			blockAreas[blockCount] = blockArea;
			++blockCount;

			// Update the list header,
			UpdateListHeaderArea();
		}

		public void DecreaseSize() {
			--blockCount;
			// Free the top block
			store.DeleteArea(blockElements[blockCount]);

			// Help the GC
			blockAreas[blockCount] = null;
			blockElements[blockCount] = 0;

			// Update the list header.
			UpdateListHeaderArea();
		}
	}
}