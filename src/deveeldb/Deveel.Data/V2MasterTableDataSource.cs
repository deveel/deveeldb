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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

using Deveel.Data.Store;
using Deveel.Data.Util;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="MasterTableDataSource"/> implementation that is backed by a 
	/// non-shared <see cref="IStore"/> object.
	/// </summary>
	/// <remarks>
	/// The store interface allows us a great deal of flexibility because
	/// we can map a store around different underlying devices.  For example, a
	/// store could map to a memory region, a memory mapped file, or a standard
	/// file.
	/// <para>
	/// The structure of the store comprises of a header block that contains the
	/// following information;
	/// <code>
	///      HEADER BLOCK
	///    +-------------------------------+
	///    | version                       |
	///    | table id                      |
	///    | table sequence id             |
	///    | pointer to DataTableInfo       |
	///    | pointer to DataIndexSetInfo    |
	///    | pointer to index block        |
	///    | LIST BLOCK HEADER pointer     |
	///    +-------------------------------+
	/// </code>
	/// </para>
	/// <para>
	/// Each record is comprised of a header which contains offsets to the fields
	/// input the record, and a serializable of the fields themselves.
	/// </para>
	/// </remarks>
	internal sealed class V2MasterTableDataSource : MasterTableDataSource {
		/// <summary>
		/// The file name of this store input the conglomerate path.
		/// </summary>
		private string fileName;
		/// <summary>
		/// The backing store object.
		/// </summary>
		private IStore store;

		/// <summary>
		/// An IndexSetStore object that manages the indexes for this table.
		/// </summary>
		private IndexSetStore indexStore;
		/// <summary>
		/// The current sequence id.
		/// </summary>
		private long sequenceId;

		/// <summary>
		/// The number of columns in this table.  This is a cached optimization.
		/// </summary>
		private int columnCount;


		// ---------- Pointers into the store ----------


		/// <summary>
		/// Points to the index header area.
		/// </summary>
		private long indexHeaderPointer;

		/// <summary>
		/// Points to the block list header area.
		/// </summary>
		private long listHeaderPointer;

		/// <summary>
		/// The header area itself.
		/// </summary>
		private IMutableArea headerArea;


		/// <summary>
		/// The structure that manages the pointers to the records.
		/// </summary>
		private FixedRecordList listStructure;

		/// <summary>
		/// The first delete chain element.
		/// </summary>
		private long firstDeleteChainRecord;

		/// <summary>
		/// Set to true when the runtime has shutdown and writes should no 
		/// longer be possible on the object.
		/// </summary>
		private bool hasShutdown;


		public V2MasterTableDataSource(TransactionSystem system, IStoreSystem storeSystem, IBlobStore blobStore)
			: base(system, storeSystem, blobStore) {
			firstDeleteChainRecord = -1;
			hasShutdown = false;
		}

		public override string SourceIdentity {
			get { return fileName; }
			set { fileName = value; }
		}

		public override int RawRowCount {
			get {
				lock (listStructure) {
					long total = listStructure.AddressableNodeCount;
					// 32-bit row limitation here - we should return a long.
					return (int) total;
				}
			}
		}

		public override long CurrentUniqueId {
			get {
				lock (listStructure) {
					return sequenceId - 1;
				}
			}
		}


		public override long GetNextUniqueId() {
			lock (listStructure) {
				long v = sequenceId;
				++sequenceId;
				if (hasShutdown)
					throw new Exception("IO operation while VM shutting down.");

				try {
					try {
						store.LockForWrite();
						headerArea.Position = 4 + 4;
						headerArea.WriteInt8(sequenceId);
						headerArea.CheckOut();
					} finally {
						store.UnlockForWrite();
					}
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message);
				}
				return v;
			}
		}

		public override bool Compact {
			get {
				// TODO: We should perform some analysis on the data to decide if a
				//   compact is necessary or not.
				return true;
			}
		}

		/// <summary>
		/// Wraps the given output stream around a buffered data output stream.
		/// </summary>
		/// <param name="output"></param>
		/// <returns></returns>
		private static BinaryWriter GetBWriter(Stream output) {
			return new BinaryWriter(new BufferedStream(output, 512), Encoding.Unicode);
		}

		/// <summary>
		/// Wraps the given input stream around a buffered data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		private static BinaryReader GetBReader(Stream input) {
			return new BinaryReader(new BufferedStream(input, 512), Encoding.Unicode);
		}

		/// <summary>
		/// Sets up an initial store (should only be called from 
		/// the <see cref="CreateTable"/> method).
		/// </summary>
		private void SetupInitialStore() {
			// Serialize the DataTableInfo object
			MemoryStream bout = new MemoryStream();
			BinaryWriter dout = new BinaryWriter(bout, Encoding.Unicode);
			dout.Write(1);
			TableInfo.Write(dout);
			// Convert to a byte array
			byte[] dataTableDefBuf = bout.ToArray();

			// Serialize the DataIndexSetInfo object
			bout = new MemoryStream();
			dout = new BinaryWriter(bout, Encoding.Unicode);
			dout.Write(1);
			IndexSetInfo.Write(dout);
			// Convert to byte array
			byte[] indexSetDefBuf = bout.ToArray();

			bout = null;
			dout = null;

			try {
				store.LockForWrite();

				// Allocate an 80 byte header
				IAreaWriter headerWriter = store.CreateArea(80);
				long headerPointer = headerWriter.Id;
				// Allocate space to store the DataTableInfo serialization
				IAreaWriter dataTableDefWriter = store.CreateArea(dataTableDefBuf.Length);
				long dataTableDefPointer = dataTableDefWriter.Id;
				// Allocate space to store the DataIndexSetInfo serialization
				IAreaWriter dataIndexSetWriter = store.CreateArea(indexSetDefBuf.Length);
				long dataIndexSetDefPointer = dataIndexSetWriter.Id;

				// Allocate space for the list header
				listHeaderPointer = listStructure.Create();
				listStructure.WriteReservedLong(-1);
				firstDeleteChainRecord = -1;

				// Create the index store
				indexStore = new IndexSetStore(store, System);
				indexHeaderPointer = indexStore.Create();

				// Write the main header
				headerWriter.WriteInt4(1);                       // Version
				headerWriter.WriteInt4(TableId);                 // table id
				headerWriter.WriteInt8(sequenceId);              // initial sequence id
				headerWriter.WriteInt8(dataTableDefPointer);     // pointer to DataTableInfo
				headerWriter.WriteInt8(dataIndexSetDefPointer);  // pointer to DataIndexSetInfo
				headerWriter.WriteInt8(indexHeaderPointer);      // index header pointer
				headerWriter.WriteInt8(listHeaderPointer);       // list header pointer
				headerWriter.Finish();

				// Write the table info
				dataTableDefWriter.Write(dataTableDefBuf);
				dataTableDefWriter.Finish();

				// Write the index set info
				dataIndexSetWriter.Write(indexSetDefBuf);
				dataIndexSetWriter.Finish();

				// Set the pointer to the header input the reserved area.
				IMutableArea fixedArea = store.GetMutableArea(-1);
				fixedArea.WriteInt8(headerPointer);
				fixedArea.CheckOut();

				// Set the header area
				headerArea = store.GetMutableArea(headerPointer);
			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Read the store headers and initialize any internal object state.
		/// </summary>
		/// <remarks>
		/// This is called by the <see cref="OpenTable"/> method.
		/// </remarks>
		private void ReadStoreHeaders() {
			// Read the fixed header
			IArea fixedArea = store.GetArea(-1);

			// Set the header area
			headerArea = store.GetMutableArea(fixedArea.ReadInt8());

			// Open a stream to the header
			int version = headerArea.ReadInt4();              // version
			if (version != 1)
				throw new IOException("Incorrect version identifier.");

			TableId = headerArea.ReadInt4();                  // table_id
			sequenceId = headerArea.ReadInt8();               // sequence id
			long infoPointer = headerArea.ReadInt8();         // pointer to DataTableInfo
			long indexInfoPointer = headerArea.ReadInt8();    // pointer to DataIndexSetInfo
			indexHeaderPointer = headerArea.ReadInt8();       // pointer to index header
			listHeaderPointer = headerArea.ReadInt8();        // pointer to list header

			// Read the table info
			BinaryReader din = GetBReader(store.GetAreaInputStream(infoPointer));
			version = din.ReadInt32();
			if (version != 1)
				throw new IOException("Incorrect DataTableInfo version identifier.");

			TableInfo = DataTableInfo.Read(din);
			din.Close();

			// Read the data index set info
			din = GetBReader(store.GetAreaInputStream(indexInfoPointer));
			version = din.ReadInt32();
			if (version != 1)
				throw new IOException("Incorrect DataIndexSetInfo version identifier.");

			IndexSetInfo = DataIndexSetInfo.Read(din);
			din.Close();

			// Read the list header
			listStructure.Init(listHeaderPointer);
			firstDeleteChainRecord = listStructure.ReadReservedLong();

			// Init the index store
			indexStore = new IndexSetStore(store, System);
			try {
				indexStore.Init(indexHeaderPointer);
			} catch (IOException) {
				// If this failed try writing output a new empty index set.
				// ISSUE: Should this occur here?  This is really an attempt at repairing
				//   the index store.
				indexStore = new IndexSetStore(store, System);
				indexHeaderPointer = indexStore.Create();
				indexStore.AddIndexLists(TableInfo.ColumnCount + 1, 1, 1024);
				headerArea.Position = 32;
				headerArea.WriteInt8(indexHeaderPointer);
				headerArea.Position = 0;
				headerArea.CheckOut();
			}

		}

		/// <summary>
		/// Creates a unique table name to give a file.
		/// </summary>
		/// <param name="system"></param>
		/// <param name="tableId">A guarenteed unique number between all tables.</param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This could be changed to suit a particular OS's style of filesystem 
		/// namespace. Or it could return some arbitarily unique number. 
		/// However, for debugging purposes it's often a good idea to return a 
		/// name that a user can recognise.
		/// </remarks>
		/// <returns></returns>
		private static string MakeSourceIdentity(TransactionSystem system, int tableId, TableName tableName) {
			string str = tableName.ToString().Replace('.', '_').ToLower();

			// Go through each character and remove each non a-z,A-Z,0-9,_ character.
			// This ensure there are no strange characters in the file name that the
			// underlying OS may not like.
			StringBuilder osifiedName = new StringBuilder();
			int count = 0;
			for (int i = 0; i < str.Length || count > 64; ++i) {
				char c = str[i];
				if ((c >= 'a' && c <= 'z') ||
					(c >= 'A' && c <= 'Z') ||
					(c >= '0' && c <= '9') ||
					c == '_') {
					osifiedName.Append(c);
					++count;
				}
			}

			return osifiedName.ToString();
		}

		/// <summary>
		/// Writes a record to the store and returns a pointer to the area 
		/// that represents the new record.
		/// </summary>
		/// <param name="data"></param>
		/// <remarks>
		/// This does not manipulate the fixed structure in any way. This method 
		/// only allocates an area to store the record and serializes the 
		/// record. It is the responsibility of the callee to add the record 
		/// into the general file structure.
		/// <para>
		/// If the <see cref="DataRow"/>contains any references to IBlob objects 
		/// then a reference count to the blob is generated at this point.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private long WriteRecordToStore(DataRow data) {
			// Calculate how much space this record will use
			int rowCells = data.ColumnCount;

			int[] cellSizes = new int[rowCells];
			int[] cellType = new int[rowCells];

			try {
				store.LockForWrite();

				// Establish a reference to any blobs input the record
				int allRecordsSize = 0;
				for (int i = 0; i < rowCells; ++i) {
					TObject cell = data.GetValue(i);
					int sz;
					int ctype;
					if (cell.Object is IRef) {
						IRef largeObjectRef = (IRef)cell.Object;
						// TBinaryType that are IBlobRef objects have to be handled separately.
						sz = 16;
						ctype = 2;
						if (largeObjectRef != null) {
							// Tell the blob store interface that we've made a static reference
							// to this blob.
							BlobStore.EstablishReference(largeObjectRef.Id);
						}
					} else {
						sz = ObjectTransfer.ExactSizeOf(cell.Object);
						ctype = 1;
					}

					cellSizes[i] = sz;
					cellType[i] = ctype;
					allRecordsSize += sz;
				}

				// Allocate space for the record,
				IAreaWriter writer = store.CreateArea(allRecordsSize + (rowCells * 8) + 4);
				long recordPointer = writer.Id;

				// The record output stream
				BinaryWriter binaryWriter = GetBWriter(writer.GetOutputStream());

				// Write the record header first,
				binaryWriter.Write(0);        // reserved for future use
				int cellSkip = 0;
				for (int i = 0; i < rowCells; ++i) {
					binaryWriter.Write(cellType[i]);
					binaryWriter.Write(cellSkip);
					cellSkip += cellSizes[i];
				}

				// Now Write a serialization of the cells themselves,
				for (int i = 0; i < rowCells; ++i) {
					TObject tObject = data.GetValue(i);
					int ctype = cellType[i];
					if (ctype == 1) {
						// Regular object
						ObjectTransfer.WriteTo(binaryWriter, tObject.Object);
					} else if (ctype == 2) {
						// This is a binary large object and must be represented as a ref
						// to a blob input the BlobStore.
						IRef largeObjectRef = (IRef)tObject.Object;
						if (largeObjectRef == null) {
							// null value
							binaryWriter.Write(1);
							binaryWriter.Write(0);                  // Reserved for future use
							binaryWriter.Write(-1L);
						} else {
							binaryWriter.Write(0);
							binaryWriter.Write(0);                  // Reserved for future use
							binaryWriter.Write(largeObjectRef.Id);
						}
					} else {
						throw new IOException("Unrecognised cell type.");
					}
				}

				// Flush the output
				binaryWriter.Flush();

				// Finish the record
				writer.Finish();

				// Return the record
				return recordPointer;
			} finally {
				store.UnlockForWrite();
			}
		}

		/// <summary>
		/// Copies the record at the given index in the source table to the 
		/// same record index in this table.
		/// </summary>
		/// <param name="src_master_table"></param>
		/// <param name="recordId"></param>
		/// <remarks>
		/// This may need to expand the fixed list record heap as necessary to 
		/// copy the record into the given position. The record is <b>not</b> 
		/// copied into the first free record position.
		/// </remarks>
		private void CopyRecordFrom(MasterTableDataSource src_master_table,int recordId) {
			// Copy the record from the source table input a DataRow object,
			int sz = src_master_table.TableInfo.ColumnCount;
			DataRow dataRow = new DataRow(System, sz);
			for (int i = 0; i < sz; ++i) {
				TObject tob = src_master_table.GetCellContents(i, recordId);
				dataRow.SetValue(i, tob);
			}

			try {
				store.LockForWrite();

				// Write record to this table but don't update any structures for the new
				// record.
				long record_p = WriteRecordToStore(dataRow);

				// Add this record into the table structure at the given index
				AddToRecordList(recordId, record_p);

				// Set the record type for this record (committed added).
				WriteRecordType(recordId, 0x010);

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Removes all blob references in the record area pointed to by <paramref name="recordPointer"/>.
		/// </summary>
		/// <param name="recordPointer"></param>
		/// <remarks>
		/// This should only be used when the record is be reclaimed.
		/// </remarks>
		private void RemoveAllBlobReferencesForRecord(long recordPointer) {
			// NOTE: Does this need to be optimized?
			IArea recordArea = store.GetArea(recordPointer);
			int reserved = recordArea.ReadInt4();  // reserved
			// Look for any blob references input the row
			for (int i = 0; i < columnCount; ++i) {
				int ctype = recordArea.ReadInt4();
				int cellOffset = recordArea.ReadInt4();
				if (ctype == 1) {
					// Type 1 is not a large object
				} else if (ctype == 2) {
					int curP = recordArea.Position;
					recordArea.Position = cellOffset + 4 + (columnCount * 8);
					int btype = recordArea.ReadInt4();
					recordArea.ReadInt4();    // (reserved)
					if (btype == 0) {
						long blobRefId = recordArea.ReadInt8();
						// Release this reference
						BlobStore.ReleaseReference(blobRefId);
					}
					// Revert the area pointer
					recordArea.Position = curP;
				} else {
					throw new Exception("Unrecognised type.");
				}
			}
		}

		/// <summary>
		/// Scans the table and drops ALL blob references in this table.
		/// </summary>
		/// <remarks>
		/// This is used when a table is dropped when is still contains elements 
		/// referenced in the <see cref="IBlobStore"/>. This will decrease the 
		/// reference count in the <see cref="IBlobStore"/> for all blobs. In 
		/// effect, this is like calling <b>delete</b> on all the data in the 
		/// table.
		/// <para>
		/// This method should only be called when the table is about to be 
		/// deleted from the file system.
		/// </para>
		/// </remarks>
		private void DropAllBlobReferences() {
			lock (listStructure) {
				long elements = listStructure.AddressableNodeCount;
				for (long i = 0; i < elements; ++i) {
					IArea a = listStructure.PositionOnNode(i);
					int status = a.ReadInt4();
					// Is the record not deleted?
					if ((status & 0x020000) == 0) {
						// Get the record pointer
						long recordPointer = a.ReadInt8();
						RemoveAllBlobReferencesForRecord(recordPointer);
					}
				}
			}
		}

		/// <summary>
		/// Checks and repairs a record if it requires repairing.
		/// </summary>
		/// <param name="rowIndex"></param>
		/// <param name="allAreas"></param>
		/// <param name="terminal"></param>
		/// <remarks>
		/// Returns true if the record is valid, or false otherwise (record is/was deleted).
		/// </remarks>
		/// <returns></returns>
		private bool CheckAndRepairRecord(int rowIndex, ICollection allAreas, IUserTerminal terminal) {
			lock (listStructure) {
				// Position input the list structure
				IMutableArea blockArea = listStructure.PositionOnNode(rowIndex);
				int p = blockArea.Position;
				int status = blockArea.ReadInt4();
				// If it is not deleted,
				if ((status & 0x020000) == 0) {
					long recordPointer = blockArea.ReadInt8();

					// Is this pointer valid?
					//TODO: check this...
					int i = new ArrayList(allAreas).BinarySearch(recordPointer);
					if (i >= 0) {
						// Pointer is valid input the store,
						// Try reading from column 0
						try {
							OnGetCellContents(0, rowIndex);
							// Return because the record is valid.
							return true;
						} catch (Exception e) {
							// If an exception is generated when accessing the data, delete the
							// record.
							terminal.WriteLine("+ Error accessing record: " + e.Message);
						}

					}

					// If we get here, the record needs to be deleted and added to the delete
					// chain
					terminal.WriteLine("+ Record area not valid: row = " + rowIndex +
					                   " pointer = " + recordPointer);
					terminal.WriteLine("+ Deleting record.");
				}

				// Put this record input the delete chain
				blockArea.Position = p;
				blockArea.WriteInt4(0x020000);
				blockArea.WriteInt8(firstDeleteChainRecord);
				blockArea.CheckOut();
				firstDeleteChainRecord = rowIndex;

				return false;
			}
		}

		/// <summary>
		/// Grows the list structure to accomodate more entries.
		/// </summary>
		/// <remarks>
		/// The new entries are added to the free chain pool. Assumes we are 
		/// synchronized over listStructure.
		/// </remarks>
		private void GrowListStructure() {
			try {
				store.LockForWrite();

				// Increase the size of the list structure.
				listStructure.IncreaseSize();

				// The start record of the new size
				int newBlockNumber = listStructure.ListBlockCount - 1;
				long startIndex = listStructure.ListBlockFirstPosition(newBlockNumber);
				long sizeOfBlock = listStructure.ListBlockNodeCount(newBlockNumber);

				// The IArea object for the new position
				IMutableArea a = listStructure.PositionOnNode(startIndex);

				// Set the rest of the block as deleted records
				for (long n = 0; n < sizeOfBlock - 1; ++n) {
					a.WriteInt4(0x020000);
					a.WriteInt8(startIndex + n + 1);
				}

				// The last block is end of delete chain.
				a.WriteInt4(0x020000);
				a.WriteInt8(firstDeleteChainRecord);
				a.CheckOut();

				// And set the new delete chain
				firstDeleteChainRecord = startIndex;

				// Set the reserved area
				listStructure.WriteReservedLong(firstDeleteChainRecord);
			} finally {
				store.UnlockForWrite();
			}
		}

		/// <summary>
		/// Adds a record to the given position in the fixed structure.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="recordPointer"></param>
		/// <remarks>
		/// If the place is already used by a record then an exception is thrown, 
		/// otherwise the record is set.
		/// </remarks>
		/// <returns></returns>
		private long AddToRecordList(long index, long recordPointer) {
			lock (listStructure) {
				if (hasShutdown)
					throw new IOException("IO operation while VM shutting down.");

				long addrCount = listStructure.AddressableNodeCount;

				// First make sure there are enough nodes to accomodate this entry,
				while (index >= addrCount) {
					GrowListStructure();
					addrCount = listStructure.AddressableNodeCount;
				}

				// Remove this from the delete chain by searching for the index input the
				// delete chain.
				long prev = -1;
				long chain = firstDeleteChainRecord;
				while (chain != -1 && chain != index) {
					IArea a1 = listStructure.PositionOnNode(chain);
					if (a1.ReadInt4() != 0x020000)
						throw new IOException("Not deleted record is input delete chain!");

					prev = chain;
					chain = a1.ReadInt8();
				}

				// Wasn't found
				if (chain == -1)
					throw new IOException("Unable to add record because index is not available.");

				// Read the next entry input the delete chain.
				IArea a = listStructure.PositionOnNode(chain);
				if (a.ReadInt4() != 0x020000)
					throw new IOException("Not deleted record is input delete chain!");

				long nextPointer = a.ReadInt8();

				try {
					store.LockForWrite();

					// If prev == -1 then first_delete_chain_record points to this record
					if (prev == -1) {
						firstDeleteChainRecord = nextPointer;
						listStructure.WriteReservedLong(firstDeleteChainRecord);
					} else {
						// Otherwise we need to set the previous node to point to the next node
						IMutableArea ma1 = listStructure.PositionOnNode(prev);
						ma1.WriteInt4(0x020000);
						ma1.WriteInt8(nextPointer);
						ma1.CheckOut();
					}

					// Finally set the recordPointer
					IMutableArea ma = listStructure.PositionOnNode(index);
					ma.WriteInt4(0);
					ma.WriteInt8(recordPointer);
					ma.CheckOut();
				} finally {
					store.UnlockForWrite();
				}
			}

			return index;
		}

		/// <summary>
		/// Finds a free place to add a record and returns an index to the 
		/// record here.
		/// </summary>
		/// <param name="recordPointer"></param>
		/// <remarks>
		/// This may expand the record space as necessary if there are no free 
		/// record slots to use.
		/// </remarks>
		/// <returns></returns>
		private long AddToRecordList(long recordPointer) {
			lock (listStructure) {
				if (hasShutdown)
					throw new IOException("IO operation while VM shutting down.");

				// If there are no free deleted records input the delete chain,
				if (firstDeleteChainRecord == -1)
					// Grow the fixed structure to allow more nodes,
					GrowListStructure();

				// Pull free block from the delete chain and recycle it.
				long recycledRecord = firstDeleteChainRecord;
				IMutableArea block = listStructure.PositionOnNode(recycledRecord);
				int recPos = block.Position;
				// Status of the recycled block
				int status = block.ReadInt4();
				if ((status & 0x020000) == 0)
					throw new ApplicationException("Assertion failed: record is not deleted.  " +
					                               "status = " + status + ", rec_pos = " + recPos);

				// The pointer to the next input the chain.
				long nextChain = block.ReadInt8();
				firstDeleteChainRecord = nextChain;

				try {
					store.LockForWrite();

					// Update the first_delete_chain_record field input the header
					listStructure.WriteReservedLong(firstDeleteChainRecord);
					// Update the block
					block.Position = recPos;
					block.WriteInt4(0);
					block.WriteInt8(recordPointer);
					block.CheckOut();
				} finally {
					store.UnlockForWrite();
				}

				return recycledRecord;
			}
		}

		protected override void CreateTable() {
			// Initially set the table sequence_id to 1
			sequenceId = 1;

			// Generate the name of the store file name.
			fileName = MakeSourceIdentity(System, TableId, TableName);

			// Create and open the store.
			store = StoreSystem.CreateStore(fileName);

			try {
				store.LockForWrite();

				// Setup the list structure
				listStructure = new FixedRecordList(store, 12);
			} finally {
				store.UnlockForWrite();
			}

			// Initialize the store to an empty state,
			SetupInitialStore();
			indexStore.AddIndexLists(TableInfo.ColumnCount + 1, (byte)1, 1024);
			columnCount = TableInfo.ColumnCount;
		}

		/// <summary>
		/// Returns true if the master table data source with the given source
		/// identity exists.
		/// </summary>
		/// <param name="identity"></param>
		/// <returns></returns>
		internal bool Exists(String identity) {
			return StoreSystem.StoreExists(identity);
		}

		protected override bool OpenTable() {
			// Open the store.
			store = StoreSystem.OpenStore(SourceIdentity);
			bool needCheck = !store.LastCloseClean();

			// Setup the list structure
			listStructure = new FixedRecordList(store, 12);

			// Read and setup the pointers
			ReadStoreHeaders();

			// Set the column count
			columnCount = TableInfo.ColumnCount;

			return needCheck;
		}

		/// <summary>
		/// Closes this master table in the file system.
		/// </summary>
		/// <param name="pendingDrop"></param>
		/// <remarks>
		/// This frees up all the resources associated with this master table.
		/// <para>
		/// This method is typically called when the database is shut down.
		/// </para>
		/// </remarks>
		internal void Close(bool pendingDrop) {
			lock (this) {
				// NOTE: This method MUST be synchronized over the table to prevent
				//   establishing a root Lock on this table.  If a root Lock is established
				//   then the collection event could fail.

				lock (listStructure) {
					// If we are root locked, we must become un root locked.
					ClearAllRootLocks();

					try {
						try {
							store.LockForWrite();

							// Force a garbage collection event.
							if (!IsReadOnly)
								TableGC.Collect(true);

							// If we are closing pending a drop, we need to remove all blob
							// references input the table.
							// NOTE: This must only happen after the above collection event.
							if (pendingDrop) {
								// Scan and remove all blob references for this dropped table.
								DropAllBlobReferences();
							}
						} finally {
							store.UnlockForWrite();
						}
					} catch (Exception e) {
						Logger.Error(this, "Exception during table (" + ToString() + ") close: " + e.Message);
						Logger.Error(this, e);
					}

					// Synchronize the store
					indexStore.Close();

					// Close the store input the store system.
					StoreSystem.CloseStore(store);

					TableInfo = null;
					IsClosed = true;
				}
			}
		}

		public override void CopyFrom(int tableId, MasterTableDataSource srcMasterTable, IIndexSet indexSet) {
			// Basically we need to copy all the data and then set the new index view.
			Create(tableId, srcMasterTable.TableInfo);

			// The record list.
			IIndex masterIndex = indexSet.GetIndex(0);

			// For each row input the master table
			int sz = srcMasterTable.RawRowCount;
			for (int i = 0; i < sz; ++i) {
				// Is this row input the set we are copying from?
				if (masterIndex.Contains(i)) {
					// Yes so copy the record into this table.
					CopyRecordFrom(srcMasterTable, i);
				}
			}

			// Copy the index set
			indexStore.CopyAllFrom(indexSet);

			// Finally set the unique id
			SetUniqueID(srcMasterTable.GetNextUniqueId());
		}

		// ---------- Diagnostic and repair ----------

		protected override void OnOpenScan() {
			// Scan for any leaks input the file,
			Logger.Info(this, "Scanning Table " + SourceIdentity + " for leaks.");
			ScanForLeaks();
		}

		/// <summary>
		/// Looks for any leaks in the file.
		/// </summary>
		/// <remarks>
		/// This works by walking through the file and index area graph and 
		/// <i>remembering</i> all areas that were read. 
		/// The store is then checked that all other areas except these are 
		/// deleted.
		/// <para>
		/// Assumes the master table is open.
		/// </para>
		/// </remarks>
		public void ScanForLeaks() {
			lock (listStructure) {
				// The list of pointers to areas (as Long).
				List<long> usedAreas = new List<long>();

				// Add the header_p pointer
				usedAreas.Add(headerArea.Id);

				headerArea.Position = 16;
				// Add the DataTableInfo and DataIndexSetInfo objects
				usedAreas.Add(headerArea.ReadInt8());
				usedAreas.Add(headerArea.ReadInt8());

				// Add all the used areas input the list_structure itself.
				listStructure.AddAllAreasUsed(usedAreas);

				// Adds all the user areas input the index store.
				indexStore.AddAllAreasUsed(usedAreas);

				// Search the list structure for all areas
				long elements = listStructure.AddressableNodeCount;
				for (long i = 0; i < elements; ++i) {
					IArea a = listStructure.PositionOnNode(i);
					int status = a.ReadInt4();
					if ((status & 0x020000) == 0) {
						long pointer = a.ReadInt8();
						//          Console.Out.WriteLine("Not deleted = " + pointer);
						// Record is not deleted,
						usedAreas.Add(pointer);
					}
				}

				// Following depends on store implementation
				if (store is AbstractStore) {
					AbstractStore aStore = (AbstractStore)store;
					List<long> leakedAreas = aStore.FindAllocatedAreasNotIn(usedAreas);
					if (leakedAreas.Count == 0) {
						Logger.Info(this, "No leaked areas.");
					} else {
						Logger.Info(this, "There were " + leakedAreas.Count + " leaked areas found.");
						foreach (long areaPointer in leakedAreas) {
							store.DeleteArea(areaPointer);
						}

						Logger.Info(this, "Leaked areas successfully freed.");
					}
				}
			}
		}

		/// <summary>
		/// Performs a complete check and repair of the table.
		/// </summary>
		/// <param name="terminal">An implementation of the user interface 
		/// <see cref="IUserTerminal"/> that is used to ask any questions 
		/// and output the results of the check.</param>
		/// <remarks>
		/// The table must not have been opened before this method is called.  
		/// </remarks>
		public override void Repair(IUserTerminal terminal) {
			terminal.WriteLine("+ Repairing V2MasterTableDataSource " + fileName);

			store = StoreSystem.OpenStore(fileName);

			// If AbstractStore then fix
			if (store is AbstractStore) {
				((AbstractStore)store).OpenScanAndFix(terminal);
			}

			// Setup the list structure
			listStructure = new FixedRecordList(store, 12);

			try {
				// Read and setup the pointers
				ReadStoreHeaders();
				// Set the column count
				columnCount = TableInfo.ColumnCount;
			} catch (IOException e) {
				// If this fails, the table is not recoverable.
				terminal.WriteLine("! Table is not repairable because the file headers are corrupt.");
				terminal.WriteLine("  Error reported: " + e.Message);
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				return;
			}

			// From here, we at least have intact headers.
			terminal.WriteLine("- Checking record integrity.");

			// Get the sorted list of all areas input the file.
			IList allAreas = store.GetAllAreas();

			// The list of all records generated when we check each record
			List<int> allRecords = new List<int>();

			// Look up each record and check it's intact,  Any records that are deleted
			// are added to the delete chain.
			firstDeleteChainRecord = -1;
			int record_count = 0;
			int freeCount = 0;
			int sz = RawRowCount;
			for (int i = sz - 1; i >= 0; --i) {
				bool recordValid = CheckAndRepairRecord(i, allAreas, terminal);
				if (recordValid) {
					allRecords.Add(i);
					++record_count;
				} else {
					++freeCount;
				}
			}

			// Set the reserved area
			listStructure.WriteReservedLong(firstDeleteChainRecord);

			terminal.Write("* Record count = " + record_count);
			terminal.WriteLine(" Free count = " + freeCount);

			// Check indexes
			terminal.WriteLine("- Rebuilding all table index information.");

			int indexCount = TableInfo.ColumnCount + 1;
			for (int i = 0; i < indexCount; ++i) {
				indexStore.CommitDropIndex(i);
			}
			
			BuildIndexes();

			terminal.WriteLine("- Table check complete.");
		}


		// ---------- Implemented from AbstractMasterTableDataSource ----------


		public override int WriteRecordType(int rowIndex, int rowState) {
			lock (listStructure) {
				if (hasShutdown)
					throw new IOException("IO operation while shutting down.");

				// Find the record entry input the block list.
				IMutableArea blockArea = listStructure.PositionOnNode(rowIndex);
				int pos = blockArea.Position;
				// Get the status.
				int oldStatus = blockArea.ReadInt4();
				int modStatus = (int)(oldStatus & 0x0FFFF0000) | (rowState & 0x0FFFF);

				// Write the new status
				try {
					store.LockForWrite();

					blockArea.Position = pos;
					blockArea.WriteInt4(modStatus);
					blockArea.CheckOut();
				} finally {
					store.UnlockForWrite();
				}

				return oldStatus & 0x0FFFF;
			}
		}


		public override int ReadRecordType(int rowIndex) {
			lock (listStructure) {
				// Find the record entry input the block list.
				IArea blockArea = listStructure.PositionOnNode(rowIndex);
				// Get the status.
				return blockArea.ReadInt4() & 0x0FFFF;
			}
		}


		protected override bool IsRecordDeleted(int rowIndex) {
			lock (listStructure) {
				// Find the record entry input the block list.
				IArea blockArea = listStructure.PositionOnNode(rowIndex);
				// If the deleted bit set for the record
				return (blockArea.ReadInt4() & 0x020000) != 0;
			}
		}


		protected override void OnDeleteRow(int rowIndex) {
			lock (listStructure) {
				if (hasShutdown)
					throw new IOException("IO operation while VM shutting down.");

				// Find the record entry input the block list.
				IMutableArea blockArea = listStructure.PositionOnNode(rowIndex);
				int p = blockArea.Position;
				int status = blockArea.ReadInt4();

				// Check it is not already deleted
				if ((status & 0x020000) != 0)
					throw new IOException("Record is already marked as deleted.");

				long recordPointer = blockArea.ReadInt8();

				// Update the status record.
				try {
					store.LockForWrite();

					blockArea.Position = p;
					blockArea.WriteInt4(0x020000);
					blockArea.WriteInt8(firstDeleteChainRecord);
					blockArea.CheckOut();
					firstDeleteChainRecord = rowIndex;

					// Update the first_delete_chain_record field input the header
					listStructure.WriteReservedLong(firstDeleteChainRecord);

					// If the record contains any references to blobs, remove the reference
					// here.
					RemoveAllBlobReferencesForRecord(recordPointer);

					// Free the record from the store
					store.DeleteArea(recordPointer);
				} finally {
					store.UnlockForWrite();
				}
			}
		}


		public override IIndexSet CreateIndexSet() {
			return indexStore.GetSnapshotIndexSet();
		}


		protected override void CommitIndexSet(IIndexSet indexSet) {
			indexStore.CommitIndexSet(indexSet);
			indexSet.Dispose();
		}

		protected override int OnAddRow(DataRow data) {
			long rowNumber;
			int intRowNumber;

			// Write the record to the store.
			lock (listStructure) {
				long recordPointer = WriteRecordToStore(data);
				// Now add this record into the record block list,
				rowNumber = AddToRecordList(recordPointer);
				intRowNumber = (int)rowNumber;
			}

			// Update the cell cache as appropriate
			if (CellCaching) {
				int rowCells = data.ColumnCount;
				for (int i = 0; i < rowCells; ++i) {
					// Put the row/column/TObject into the cache.
					Cache.Set(TableId, intRowNumber, i, data.GetValue(i));
				}
			}

			// Return the record index of the new data input the table
			// NOTE: We are casting this from a long to int which means we are limited
			//   to ~2 billion record references.
			return (int)rowNumber;
		}


		protected override void CheckForCleanup() {
			lock (this) {
				TableGC.Collect(false);
			}
		}


		//  private short s_run_total_hits = 0;
		private short sRunFileHits = Int16.MaxValue;

		protected override TObject OnGetCellContents(int column, int row) {

			// NOTES:
			// This is called *A LOT*.  It's a key part of the 20% of the program
			// that's run 80% of the time.
			// This performs very nicely for rows that are completely contained within
			// 1 sector.  However, rows that contain large cells (eg. a large binary
			// or a large string) and spans many sectors will not be utilizing memory
			// as well as it could.
			// The reason is because all the data for a row is Read from the store even
			// if only 1 cell of the column is requested.  This will have a big
			// impact on column scans and searches.  The cell cache takes some of this
			// performance bottleneck away.
			// However, a better implementation of this method is made difficult by
			// the fact that sector spans can be compressed.  We should perhaps
			// revise the low level data storage so only sectors can be compressed.

			//    // If the database stats need updating then do so now.
			//    if (s_run_total_hits >= 1600) {
			//      System.Stats.Add(s_run_total_hits, total_hits_key);
			//      System.Stats.Add(s_run_file_hits, file_hits_key);
			//      s_run_total_hits = 0;
			//      s_run_file_hits = 0;
			//    }

			//    // Increment the total hits counter
			//    ++s_run_total_hits;

			// First check if this is within the cache before we continue.
			TObject cell;
			if (CellCaching) {
				cell = Cache.Get(TableId, row, column);
				if (cell != null)
					return cell;
			}

			// We maintain a cache of byte[] arrays that contain the rows Read input
			// from the file.  If consequtive reads are made to the same row, then
			// this will cause lots of fast cache hits.

			long recordPointer = -1;
			try {
				lock (listStructure) {
					// Increment the file hits counter
					++sRunFileHits;

					if (sRunFileHits >= 100) {
						System.Stats.Add(sRunFileHits, FileHitsKey);
						sRunFileHits = 0;
					}

					// Get the node for the record
					IArea listBlock = listStructure.PositionOnNode(row);
					int status = listBlock.ReadInt4();
					// Check it's not deleted
					if ((status & 0x020000) != 0)
						throw new ApplicationException("Unable to Read deleted record.");

					// Get the pointer to the record we are reading
					recordPointer = listBlock.ReadInt8();
				}

				// Open a stream to the record
				BinaryReader reader = GetBReader(store.GetAreaInputStream(recordPointer));

				// SkipStream(din.BaseStream, 4 + (column * 8));
				reader.BaseStream.Seek(4 + (column*8), SeekOrigin.Current);
				int cellType = reader.ReadInt32();
				int cellOffset = reader.ReadInt32();

				int curAt = 8 + 4 + (column * 8);
				int beAt = 4 + (columnCount * 8);
				int skipAmount = (beAt - curAt) + cellOffset;

				// SkipStream(din.BaseStream, skip_amount);
				reader.BaseStream.Seek(skipAmount, SeekOrigin.Current);

				// Get the TType for this column
				// NOTE: It's possible this call may need optimizing?
				TType ttype = TableInfo[column].TType;

				object ob;
				if (cellType == 1) {
					// If standard object type
					ob = ObjectTransfer.ReadFrom(reader);
				} else if (cellType == 2) {
					// If reference to a blob input the BlobStore
					int fType = reader.ReadInt32();
					int fReserved = reader.ReadInt32();
					long refId = reader.ReadInt64();

					if (fType == 0) {
						// Resolve the reference
						ob = BlobStore.GetLargeObject(refId);
					} else if (fType == 1) {
						ob = null;
					} else {
						throw new Exception("Unknown blob type.");
					}
				} else {
					throw new Exception("Unrecognised cell type input data.");
				}

				// Wrap it around a TObject
				cell = new TObject(ttype, ob);

				// And close the reader.
				reader.Close();
			} catch (IOException e) {
				Logger.Error(this, e);
				throw new Exception("IOError getting cell at (" + column + ", " + row + ") pointer = " + recordPointer + ".");
			}

			// And WriteByte input the cache and return it.
			if (CellCaching) {
				Cache.Set(TableId, row, column, cell);
			}

			return cell;
		}


		public override void SetUniqueID(long value) {
			lock (listStructure) {
				sequenceId = value;
				if (hasShutdown) {
					throw new Exception("IO operation while VM shutting down.");
				}
				try {
					try {
						store.LockForWrite();
						headerArea.Position = 4 + 4;
						headerArea.WriteInt8(sequenceId);
						headerArea.CheckOut();
					} finally {
						store.UnlockForWrite();
					}
				} catch (IOException e) {
					Logger.Error(this, e);
					throw new ApplicationException("IO Error: " + e.Message, e);
				}
			}
		}

		public override void Dispose(bool pendingDrop) {
			lock (this) {
				lock (listStructure) {
					if (!IsClosed) {
						Close(pendingDrop);
					}
				}
			}
		}

		public override bool Drop() {
			lock (this) {
				lock (listStructure) {
					if (!IsClosed)
						Close(true);

					bool deleted = StoreSystem.DeleteStore(store);
					if (deleted)
						Logger.Message(this, "Dropped: " + SourceIdentity);

					return deleted;

				}
			}
		}

		public override void ShutdownHookCleanup() {
			lock (listStructure) {
				indexStore.Close();
				hasShutdown = true;
			}
		}
	}
}