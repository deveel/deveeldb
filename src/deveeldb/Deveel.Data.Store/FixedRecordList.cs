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

namespace Deveel.Data.Store {
	public sealed class FixedRecordList {
		private const int Magic = 0x087131AA;

		private readonly IStore store;
		private readonly int elementSize;

		private long headerAreaId;
		private IArea headerArea;

		// Pointers to the blocks in the list block.
		private readonly long[] blockElements;
		private readonly IArea[] blockAreas;

		public FixedRecordList(IStore store, int elementSize) {
			this.store = store;
			this.elementSize = elementSize;
			blockElements = new long[64];
			blockAreas = new IArea[64];
		}

		public int BlockCount { get; private set; }

		public long NodeCount {
			get { return BlockFirstPosition(BlockCount); }
		}

		private void UpdateListHeaderArea() {
			headerArea.Position = 4;
			headerArea.WriteInt4(BlockCount);
			headerArea.Position = 16;
			for (int i = 0; i < BlockCount; ++i) {
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
			BlockCount = 0;
			UpdateListHeaderArea();

			return headerAreaId;
		}

		public void Open(long listPointer) {
			headerAreaId = listPointer;
			headerArea = store.GetArea(headerAreaId);

			int magic = headerArea.ReadInt4(); // MAGIC
			if (magic != Magic)
				throw new IOException("Incorrect magic for list block. [magic=" + magic + "]");

			BlockCount = headerArea.ReadInt4();
			headerArea.ReadInt8(); // Delete Chain Head
			for (int i = 0; i < BlockCount; ++i) {
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
			long sizeOfBlock = 32L << BlockCount;

			// Allocate the new block in the store
			var area = store.CreateArea(sizeOfBlock * elementSize);
			var blockId = area.Id;
			area.Flush();

			var blockArea = store.GetArea(blockId);

			// Update the block list
			blockElements[BlockCount] = blockId;
			blockAreas[BlockCount] = blockArea;
			++BlockCount;

			// Update the list header,
			UpdateListHeaderArea();
		}

		public void DecreaseSize() {
			--BlockCount;
			// Free the top block
			store.DeleteArea(blockElements[BlockCount]);

			// Help the GC
			blockAreas[BlockCount] = null;
			blockElements[BlockCount] = 0;

			// Update the list header.
			UpdateListHeaderArea();
		}

		public void GetAreasUsed(IList<long> usedAreas) {
			usedAreas.Add(headerAreaId);
			for (int i = 0; i < BlockCount; ++i) {
				usedAreas.Add(blockElements[i]);
			}
		}
	}
}