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
using System.IO;
using System.IO.Compression;

namespace Deveel.Data.Store {
	public class ObjectStore : IObjectStore {
		private const int Magic = 0x012BC53A9;

		private const int DeletedFlag = 0x020000;
		private const int CompressedFlag = 0x010;

		private const int PageSize = 64;

		private readonly IStore store;
		private readonly FixedRecordList fixedList;

		private long firstDeleteChainRecord;

		public ObjectStore(int id, IStore store) {
			if (id < 0)
				throw new ArgumentOutOfRangeException("id");
			if (store == null)
				throw new ArgumentNullException("store");

			Id = id;
			this.store = store;
			fixedList = new FixedRecordList(store, 24);
		}

		private long AddToRecordList(long recordOffset) {
			lock (fixedList) {
				// If there is no free deleted records in the delete chain,
				if (firstDeleteChainRecord == -1) {
					// Increase the size of the list structure.
					fixedList.IncreaseSize();
					// The start record of the new size
					int newBlockNumber = fixedList.BlockCount - 1;
					long startIndex = fixedList.BlockFirstPosition(newBlockNumber);
					long sizeOfBlock = fixedList.BlockNodeCount(newBlockNumber);

					// The IArea object for the new position
					IMutableArea a = fixedList.GetRecord(startIndex);

					a.WriteInt4(0);
					a.WriteInt4(0);
					a.WriteInt8(-1);  // Initially unknown size
					a.WriteInt8(recordOffset);
					// Set the rest of the block as deleted records
					for (long n = 1; n < sizeOfBlock - 1; ++n) {
						a.WriteInt4(DeletedFlag);
						a.WriteInt4(0);
						a.WriteInt8(-1);
						a.WriteInt8(startIndex + n + 1);
					}
					// The last block is end of delete chain.
					a.WriteInt4(DeletedFlag);
					a.WriteInt4(0);
					a.WriteInt8(-1);
					a.WriteInt8(-1);
					// Check out the changes.
					a.CheckOut();

					// And set the new delete chain
					firstDeleteChainRecord = startIndex + 1;
					fixedList.WriteDeleteHead(firstDeleteChainRecord);

					// Return pointer to the record we just added.
					return startIndex;

				}

				// Pull free block from the delete chain and recycle it.
				long recycledRecord = firstDeleteChainRecord;
				IMutableArea block = fixedList.GetRecord(recycledRecord);
				int recordPos = block.Position;
				// Status of the recycled block
				int status = block.ReadInt4();
				if ((status & DeletedFlag) == 0)
					throw new ApplicationException("Assertion failed: record is not deleted!");

				// Reference count (currently unused in delete chains).
				block.ReadInt4();
				// The size (should be -1);
				block.ReadInt8();
				// The pointer to the next in the chain.
				long nextChain = block.ReadInt8();
				firstDeleteChainRecord = nextChain;

				// Update the first_delete_chain_record field in the header
				fixedList.WriteDeleteHead(firstDeleteChainRecord);

				// Update the block
				block.Position = recordPos;
				block.WriteInt4(0);
				block.WriteInt4(0);
				block.WriteInt8(-1);    // Initially unknown size
				block.WriteInt8(recordOffset);

				// Check out the changes
				block.CheckOut();

				return recycledRecord;
			}
		}

		public long Create() {
			// Init the fixed record list area.
			// The fixed list entries are formatted as follows;
			//  ( status (int), reference_count (int),
			//    blob_size (long), blob_pointer (long) )
			long fixedListOffset = fixedList.Create();

			// Delete chain is empty when we start
			firstDeleteChainRecord = -1;
			fixedList.WriteDeleteHead(-1);

			// Allocate a small header that contains the MAGIC, and the pointer to the
			// fixed list structure.
			IAreaWriter blobStoreHeader = store.CreateArea(32);
			long blobStoreId = blobStoreHeader.Id;

			blobStoreHeader.WriteInt4(Magic);	// Magic
			blobStoreHeader.WriteInt4(1);		// The data version
			blobStoreHeader.WriteInt8(fixedListOffset);
			blobStoreHeader.Finish();

			// Return the pointer to the blob store header
			return blobStoreId;
		}

		public void Open(long offset) {
			// Get the header area
			IArea headerArea = store.GetArea(offset);
			headerArea.Position = 0;

			// Read the magic
			int magic = headerArea.ReadInt4();
			int version = headerArea.ReadInt4();
			if (magic != Magic)
				throw new IOException("The magic value for this Object Store is not correct.");
			if (version != 1)
				throw new IOException("The version number for this Object Store is not correct.");

			long fixedListOffset = headerArea.ReadInt8();
			fixedList.Open(fixedListOffset);

			// Set the delete chain
			firstDeleteChainRecord = fixedList.ReadDeleteHead();
		}

		public void Dispose() {
		}

		public int Id { get; private set; }

		public ILargeObject CreateNewObject(long maxSize, bool compressed) {
			if (maxSize < 0)
				throw new IOException("Negative object size not allowed.");

			try {
				store.LockForWrite();

				// Allocate the area (plus header area) for storing the blob pages
				long pageCount = ((maxSize - 1) / (PageSize * 1024)) + 1;
				IAreaWriter objArea = store.CreateArea((pageCount * 8) + 24);
				long objAreaId = objArea.Id;

				var type = 2;			// Binary Type
				if (compressed)
					type |= CompressedFlag;

				// Set up the area header
				objArea.WriteInt4(0);           // Reserved for future
				objArea.WriteInt4(type);
				objArea.WriteInt8(maxSize);
				objArea.WriteInt8(pageCount);

				// Initialize the empty blob area
				for (long i = 0; i < pageCount; ++i) {
					objArea.WriteInt8(-1);
				}

				// And finish
				objArea.Finish();

				// Update the fixed_list and return the record number for this blob
				long refId = AddToRecordList(objAreaId);
				return new LargeObject(this, refId, maxSize, compressed, false);
			} finally {
				store.UnlockForWrite();
			}
		}

		public class LargeObject : ILargeObject {
			private readonly ObjectStore store;

			public LargeObject(ObjectStore store, long refId, long size, bool compressed, bool isComplete) {
				this.store = store;
				RawSize = size;
				Id = new ObjectId(store.Id, refId);
				IsCompressed = compressed;
				IsComplete = isComplete;
			}

			public void Dispose() {
			}

			public ObjectId Id { get; private set; }

			public long RawSize { get; private set; }

			public bool IsCompressed { get; private set; }

			public bool IsComplete { get; private set; }

			public int Read(long offset, byte[] buffer, int length) {
				return store.ReadObjectPart(Id.Id, offset, buffer, 0, length);
			}

			public void Write(long offset, byte[] buffer, int length) {
				if (IsComplete)
					throw new IOException("The object is complete and cannot be written.");

				store.WriteObjectPart(Id.Id, offset, buffer, 0, length);
			}

			public void Complete() {
				store.CompleteObject(this);
			}
		}

		private void CompleteObject(LargeObject obj) {
			throw new NotImplementedException();
		}

		private void WriteObjectPart(long id, long objOffset, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		private int ReadObjectPart(long id, long objOffset, byte[] buffer, int off, int length) {
			// ASSERT: Read and Write position must be 64K aligned.
			if (off % (64 * 1024) != 0) {
				throw new Exception("Assert failed: offset is not 64k aligned.");
			}
			// ASSERT: Length is less than or equal to 64K
			if (length > (64 * 1024)) {
				throw new Exception("Assert failed: length is greater than 64K.");
			}

			int status;
			int refCount;
			long size;
			long objPointer;

			lock (fixedList) {
				// Assert that the blob reference id given is a valid range
				if (id < 0 || id >= fixedList.NodeCount) {
					throw new IOException("Object ID is out of range.");
				}

				// Position on this record
				IArea block = fixedList.GetRecord(id);
				// Read the information in the fixed record
				status = block.ReadInt4();
				// Assert that the status is not deleted
				if ((status & DeletedFlag) != 0)
					throw new ApplicationException("Assertion failed: record is deleted!");

				// Get the reference count
				refCount = block.ReadInt4();
				// Get the total size of the object
				size = block.ReadInt8();
				// Get the blob pointer
				objPointer = block.ReadInt8();

			}

			// Assert that the area being Read is within the bounds of the object
			if (off < 0 || off + length > size) {
				throw new IOException("Invalid Read.  offset = " + off + ", length = " + length);
			}

			// Open an IArea into the object
			IArea area = store.GetArea(objPointer);
			area.ReadInt4();
			int type = area.ReadInt4();

			// Convert to the page number
			long pageNumber = (objOffset / (64 * 1024));
			area.Position = (int)((pageNumber * 8) + 24);
			long pagePointer = area.ReadInt8();

			// Read the page
			IArea pageArea = store.GetArea(pagePointer);
			pageArea.Position = 0;
			int pageType = pageArea.ReadInt4();
			int pageSize = pageArea.ReadInt4();

			if ((type & CompressedFlag) != 0) {
				// The page is compressed
				byte[] pageBuf = new byte[pageSize];
				int readCount = pageArea.Read(pageBuf, 0, pageSize);

				var deflateStream = new DeflateStream(new MemoryStream(pageBuf, 0, pageSize), CompressionMode.Decompress, false);
				try {
					int resultLength = deflateStream.Read(buffer, off, length);
					if (resultLength != length)
						throw new Exception("Assert failed: decompressed length is incorrect.");

					return readCount;
				} catch(InvalidDataException e) {
					throw new IOException("ZIP Data Format Error: " + e.Message);
				}
			}

			// The page is not compressed
			return pageArea.Read(buffer, off, length);
		}

		public ILargeObject GetObject(ObjectId id) {
			long objOffset;
			long size;
			lock (fixedList) {
				if (id.StoreId != Id)
					throw new InvalidObjectIdException(id);

				var refId = id.Id;
				// Assert that the blob reference id given is a valid range
				if (refId < 0 || refId >= fixedList.NodeCount)
					throw new InvalidObjectIdException(id);

				// Position on this record
				IArea block = fixedList.GetRecord(refId);
				// Read the information in the fixed record
				int status = block.ReadInt4();
				// Assert that the status is not deleted
				if ((status & DeletedFlag) != 0)
					throw new ApplicationException("Assertion failed: record is deleted!");

				// Get the reference count
				int refCount = block.ReadInt4();
				// Get the total size of the blob
				size = block.ReadInt8();
				// Get the blob pointer
				objOffset = block.ReadInt8();
			}

			IArea area = store.GetArea(objOffset);
			area.Position = 0;
			area.ReadInt4();  // (reserved)
			// Read the type
			int type = area.ReadInt4();
			// The size of the block
			long blockSize = area.ReadInt8();
			// The number of pages in the blob
			long pageCount = area.ReadInt8();

			bool compressed = (type & CompressedFlag) != 0;
			return new LargeObject(this, id.Id, size, compressed, true);
		}

		public void EstablishReference(ObjectId id) {
			throw new NotImplementedException();
		}

		public void ReleaseReference(ObjectId id) {
			throw new NotImplementedException();
		}
	}
}