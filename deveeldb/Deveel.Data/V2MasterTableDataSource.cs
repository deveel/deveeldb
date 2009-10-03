// 
//  V2MasterTableDataSource.cs
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
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Data.Collections;
using Deveel.Data.Store;
using Deveel.Data.Util;
using Deveel.Diagnostics;

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
	///    | pointer to DataTableDef       |
	///    | pointer to DataIndexSetDef    |
	///    | pointer to index block        |
	///    | LIST BLOCK HEADER pointer     |
	///    +-------------------------------+
	/// </code>
	/// <para>
	/// Each record is comprised of a header which contains offsets to the fields
	/// input the record, and a serializable of the fields themselves.
	/// </para>
	/// </remarks>
	internal sealed class V2MasterTableDataSource : MasterTableDataSource {
		/// <summary>
		/// The file name of this store input the conglomerate path.
		/// </summary>
		private String file_name;
		/// <summary>
		/// The backing store object.
		/// </summary>
		private IStore store;

		/// <summary>
		/// An IndexSetStore object that manages the indexes for this table.
		/// </summary>
		private IndexSetStore index_store;
		/// <summary>
		/// The current sequence id.
		/// </summary>
		private long sequence_id;


		// ---------- Pointers into the store ----------

		//  /// <summary>
		//  /// Points to the store header area.
		//  /// </summary>
		//  private long header_p;

		/// <summary>
		/// Points to the index header area.
		/// </summary>
		private long index_header_p;

		/// <summary>
		/// Points to the block list header area.
		/// </summary>
		private long list_header_p;

		/// <summary>
		/// The header area itself.
		/// </summary>
		private IMutableArea header_area;


		/// <summary>
		/// The structure that manages the pointers to the records.
		/// </summary>
		private FixedRecordList list_structure;

		/// <summary>
		/// The first delete chain element.
		/// </summary>
		private long first_delete_chain_record;

		/// <summary>
		/// Set to true when the runtime has shutdown and writes should no 
		/// longer be possible on the object.
		/// </summary>
		private bool has_shutdown;


		/**
		 * The Constructor.
		 */
		public V2MasterTableDataSource(TransactionSystem system,
									   IStoreSystem store_system,
									   OpenTransactionList open_transactions,
									   IBlobStore blob_store)
			: base(system, store_system, open_transactions, blob_store) {
			first_delete_chain_record = -1;
			has_shutdown = false;
		}

		/// <summary>
		/// Wraps the given output stream around a buffered data output stream.
		/// </summary>
		/// <param name="output"></param>
		/// <returns></returns>
		private static BinaryWriter GetBWriter(Stream output) {
			return new BinaryWriter(new BufferedStream(output, 512), Encoding.UTF8);
		}

		/// <summary>
		/// Wraps the given input stream around a buffered data input stream.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		private static BinaryReader GetBReader(Stream input) {
			return new BinaryReader(new BufferedStream(input, 512), Encoding.UTF8);
		}

		/// <summary>
		/// Sets up an initial store (should only be called from 
		/// the <see cref="Create"/> method).
		/// </summary>
		private void SetupInitialStore() {
			// Serialize the DataTableDef object
			MemoryStream bout = new MemoryStream();
			BinaryWriter dout = new BinaryWriter(bout, Encoding.UTF8);
			dout.Write(1);
			DataTableDef.Write(dout);
			// Convert to a byte array
			byte[] data_table_def_buf = bout.ToArray();

			// Serialize the DataIndexSetDef object
			bout = new MemoryStream();
			dout = new BinaryWriter(bout, Encoding.UTF8);
			dout.Write(1);
			DataIndexSetDef.Write(dout);
			// Convert to byte array
			byte[] index_set_def_buf = bout.ToArray();

			bout = null;
			dout = null;

			try {
				store.LockForWrite();

				// Allocate an 80 byte header
				IAreaWriter header_writer = store.CreateArea(80);
				long header_p = header_writer.Id;
				// Allocate space to store the DataTableDef serialization
				IAreaWriter data_table_def_writer =
										   store.CreateArea(data_table_def_buf.Length);
				long data_table_def_p = data_table_def_writer.Id;
				// Allocate space to store the DataIndexSetDef serialization
				IAreaWriter data_index_set_writer =
											store.CreateArea(index_set_def_buf.Length);
				long data_index_set_def_p = data_index_set_writer.Id;

				// Allocate space for the list header
				list_header_p = list_structure.Create();
				list_structure.WriteReservedLong(-1);
				first_delete_chain_record = -1;

				// Create the index store
				index_store = new IndexSetStore(store, System);
				index_header_p = index_store.Create();

				// Write the main header
				header_writer.WriteInt4(1);                  // Version
				header_writer.WriteInt4(table_id);           // table_id
				header_writer.WriteInt8(sequence_id);       // initial sequence id
				header_writer.WriteInt8(data_table_def_p);  // pointer to DataTableDef
				header_writer.WriteInt8(data_index_set_def_p); // pointer to DataIndexSetDef
				header_writer.WriteInt8(index_header_p);    // index header pointer
				header_writer.WriteInt8(list_header_p);     // list header pointer
				header_writer.Finish();

				// Write the data_table_def
				data_table_def_writer.Write(data_table_def_buf);
				data_table_def_writer.Finish();

				// Write the data_index_set_def
				data_index_set_writer.Write(index_set_def_buf);
				data_index_set_writer.Finish();

				// Set the pointer to the header input the reserved area.
				IMutableArea fixed_area = store.GetMutableArea(-1);
				fixed_area.WriteInt8(header_p);
				fixed_area.CheckOut();

				// Set the header area
				header_area = store.GetMutableArea(header_p);

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Read the store headers and initialize any internal object state.
		/// </summary>
		/// <remarks>
		/// This is called by the <see cref="Open"/> method.
		/// </remarks>
		private void ReadStoreHeaders() {
			// Read the fixed header
			IArea fixed_area = store.GetArea(-1);
			// Set the header area
			header_area = store.GetMutableArea(fixed_area.ReadInt8());

			// Open a stream to the header
			int version = header_area.ReadInt4();              // version
			if (version != 1) {
				throw new IOException("Incorrect version identifier.");
			}
			this.table_id = header_area.ReadInt4();         // table_id
			this.sequence_id = header_area.ReadInt8();     // sequence id
			long def_p = header_area.ReadInt8();           // pointer to DataTableDef
			long index_def_p = header_area.ReadInt8();     // pointer to DataIndexSetDef
			this.index_header_p = header_area.ReadInt8();  // pointer to index header
			this.list_header_p = header_area.ReadInt8();   // pointer to list header

			// Read the data table def
			BinaryReader din = GetBReader(store.GetAreaInputStream(def_p));
			version = din.ReadInt32();
			if (version != 1) {
				throw new IOException("Incorrect DataTableDef version identifier.");
			}
			table_def = DataTableDef.Read(din);
			din.Close();

			// Read the data index set def
			din = GetBReader(store.GetAreaInputStream(index_def_p));
			version = din.ReadInt32();
			if (version != 1) {
				throw new IOException("Incorrect DataIndexSetDef version identifier.");
			}
			index_def = DataIndexSetDef.Read(din);
			din.Close();

			// Read the list header
			list_structure.Init(list_header_p);
			first_delete_chain_record = list_structure.ReadReservedLong();

			// Init the index store
			index_store = new IndexSetStore(store, System);
			try {
				index_store.Init(index_header_p);
			} catch (IOException e) {
				// If this failed try writing output a new empty index set.
				// ISSUE: Should this occur here?  This is really an attempt at repairing
				//   the index store.
				index_store = new IndexSetStore(store, System);
				index_header_p = index_store.Create();
				index_store.AddIndexLists(table_def.ColumnCount + 1, (byte)1, 1024);
				header_area.Position = 32;
				header_area.WriteInt8(index_header_p);
				header_area.Position = 0;
				header_area.CheckOut();
			}

		}

		/// <summary>
		/// Create this master table input the file system at the given path.
		/// </summary>
		/// <param name="table_id"></param>
		/// <param name="table_def"></param>
		/// <remarks>
		/// This will initialise the various file objects and result input a new empty 
		/// master table to store data input.
		/// </remarks>
		public void Create(int table_id, DataTableDef table_def) {

			// Set the data table def object
			SetupDataTableDef(table_def);

			// Initially set the table sequence_id to 1
			this.sequence_id = 1;

			// Generate the name of the store file name.
			this.file_name = MakeTableFileName(System, table_id, TableName);

			// Create and open the store.
			store = StoreSystem.CreateStore(file_name);

			try {
				store.LockForWrite();

				// Setup the list structure
				list_structure = new FixedRecordList(store, 12);
			} finally {
				store.UnlockForWrite();
			}

			// Set up internal state of this object
			this.table_id = table_id;

			// Initialize the store to an empty state,
			SetupInitialStore();
			index_store.AddIndexLists(table_def.ColumnCount + 1, (byte)1, 1024);

			// Load internal state
			LoadInternal();

			//    synchAll();

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

		/// <summary>
		/// Opens an existing master table from the file system at the 
		/// path of the conglomerate this belongs to.
		/// </summary>
		/// <param name="file_name"></param>
		/// <remarks>
		/// This will set up the internal state of this object with the 
		/// data read input.
		/// </remarks>
		public void Open(String file_name) {

			// Set Read only flag.
			this.file_name = file_name;

			// Open the store.
			store = StoreSystem.OpenStore(file_name);
			bool need_check = !store.LastCloseClean();

			// Setup the list structure
			list_structure = new FixedRecordList(store, 12);

			// Read and setup the pointers
			ReadStoreHeaders();

			// Set the column count
			column_count = table_def.ColumnCount;

			// Open table indices
			table_indices = new MultiVersionTableIndices(System,
								   table_def.TableName, table_def.ColumnCount);
			// The column rid list cache
			column_rid_list = new RIDList[table_def.ColumnCount];

			// Load internal state
			LoadInternal();

			if (need_check) {
				// Do an opening scan of the table.  Any records that are uncommited
				// must be marked as deleted.
				DoOpeningScan();

				// Scan for any leaks input the file,
				Debug.Write(DebugLevel.Information, this,
							  "Scanning File: " + file_name + " for leaks.");
				ScanForLeaks();
			}
		}

		/// <summary>
		/// Closes this master table in the file system.
		/// </summary>
		/// <param name="pending_drop"></param>
		/// <remarks>
		/// This frees up all the resources associated with this master table.
		/// <para>
		/// This method is typically called when the database is shut down.
		/// </para>
		/// </remarks>
		internal void Close(bool pending_drop) {
			lock (this) {
				// NOTE: This method MUST be synchronized over the table to prevent
				//   establishing a root Lock on this table.  If a root Lock is established
				//   then the collection event could fail.

				lock (list_structure) {

					// If we are root locked, we must become un root locked.
					ClearAllRootLocks();

					try {
						try {
							store.LockForWrite();

							// Force a garbage collection event.
							if (!IsReadOnly) {
								gc.Collect(true);
							}

							// If we are closing pending a drop, we need to remove all blob
							// references input the table.
							// NOTE: This must only happen after the above collection event.
							if (pending_drop) {
								// Scan and remove all blob references for this dropped table.
								DropAllBlobReferences();
							}
						} finally {
							store.UnlockForWrite();
						}
					} catch (Exception e) {
						Debug.Write(DebugLevel.Error, this, "Exception during table (" + ToString() + ") close: " + e.Message);
						Debug.WriteException(e);
					}

					// Synchronize the store
					index_store.Close();
					//      store.flush();

					// Close the store input the store system.
					StoreSystem.CloseStore(store);

					table_def = null;
					table_indices = null;
					column_rid_list = null;
					is_closed = true;
				}
			}
		}

		/// <summary>
		/// Creates a new master table data source that is a copy of the 
		/// given <see cref="MasterTableDataSource"/> object.
		/// </summary>
		/// <param name="table_id">The table id to given the new table.</param>
		/// <param name="src_master_table">The table to copy.</param>
		/// <param name="index_set">The view of the table to be copied.</param>
		internal void Copy(int table_id, MasterTableDataSource src_master_table, IIndexSet index_set) {

			// Basically we need to copy all the data and then set the new index view.
			Create(table_id, src_master_table.DataTableDef);

			// The record list.
			IIntegerList master_index = index_set.GetIndex(0);

			// For each row input the master table
			int sz = src_master_table.RawRowCount;
			for (int i = 0; i < sz; ++i) {
				// Is this row input the set we are copying from?
				if (master_index.Contains(i)) {
					// Yes so copy the record into this table.
					CopyRecordFrom(src_master_table, i);
				}
			}

			// Copy the index set
			index_store.CopyAllFrom(index_set);

			// Finally set the unique id
			long un_id = src_master_table.NextUniqueId;
			SetUniqueID(un_id);

		}

		// ---------- Low level operations ----------

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
		/// If the <see cref="RowData"/>contains any references to IBlob objects 
		/// then a reference count to the blob is generated at this point.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private long WriteRecordToStore(RowData data) {

			// Calculate how much space this record will use
			int row_cells = data.ColumnCount;

			int[] cell_sizes = new int[row_cells];
			int[] cell_type = new int[row_cells];

			try {
				store.LockForWrite();

				// Establish a reference to any blobs input the record
				int all_records_size = 0;
				for (int i = 0; i < row_cells; ++i) {
					TObject cell = data.GetCellData(i);
					int sz;
					int ctype;
					if (cell.Object is IRef) {
						IRef large_object_ref = (IRef)cell.Object;
						// TBinaryType that are IBlobRef objects have to be handled separately.
						sz = 16;
						ctype = 2;
						if (large_object_ref != null) {
							// Tell the blob store interface that we've made a static reference
							// to this blob.
							blob_store.EstablishReference(large_object_ref.Id);
						}
					} else {
						sz = ObjectTransfer.ExactSizeOf(cell.Object);
						ctype = 1;
					}
					cell_sizes[i] = sz;
					cell_type[i] = ctype;
					all_records_size += sz;
				}

				long record_p;

				// Allocate space for the record,
				IAreaWriter writer =
							   store.CreateArea(all_records_size + (row_cells * 8) + 4);
				record_p = writer.Id;

				// The record output stream
				BinaryWriter dout = GetBWriter(writer.GetOutputStream());

				// Write the record header first,
				dout.Write(0);        // reserved for future use
				int cell_skip = 0;
				for (int i = 0; i < row_cells; ++i) {
					dout.Write((int)cell_type[i]);
					dout.Write(cell_skip);
					cell_skip += cell_sizes[i];
				}

				// Now Write a serialization of the cells themselves,
				for (int i = 0; i < row_cells; ++i) {
					TObject t_object = data.GetCellData(i);
					int ctype = cell_type[i];
					if (ctype == 1) {
						// Regular object
						ObjectTransfer.WriteTo(dout, t_object.Object);
					} else if (ctype == 2) {
						// This is a binary large object and must be represented as a ref
						// to a blob input the BlobStore.
						IRef large_object_ref = (IRef)t_object.Object;
						if (large_object_ref == null) {
							// null value
							dout.Write(1);
							dout.Write(0);                  // Reserved for future use
							dout.Write(-1L);
						} else {
							dout.Write(0);
							dout.Write(0);                  // Reserved for future use
							dout.Write(large_object_ref.Id);
						}
					} else {
						throw new IOException("Unrecognised cell type.");
					}
				}

				// Flush the output
				dout.Flush();

				// Finish the record
				writer.Finish();

				// Return the record
				return record_p;

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Copies the record at the given index in the source table to the 
		/// same record index in this table.
		/// </summary>
		/// <param name="src_master_table"></param>
		/// <param name="record_id"></param>
		/// <remarks>
		/// This may need to expand the fixed list record heap as necessary to 
		/// copy the record into the given position. The record is <b>not</b> 
		/// copied into the first free record position.
		/// </remarks>
		private void CopyRecordFrom(MasterTableDataSource src_master_table,int record_id) {

			// Copy the record from the source table input a RowData object,
			int sz = src_master_table.DataTableDef.ColumnCount;
			RowData row_data = new RowData(System, sz);
			for (int i = 0; i < sz; ++i) {
				TObject tob = src_master_table.GetCellContents(i, record_id);
				row_data.SetColumnDataFromTObject(i, tob);
			}

			try {
				store.LockForWrite();

				// Write record to this table but don't update any structures for the new
				// record.
				long record_p = WriteRecordToStore(row_data);

				// Add this record into the table structure at the given index
				AddToRecordList(record_id, record_p);

				// Set the record type for this record (committed added).
				WriteRecordType(record_id, 0x010);

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Removes all blob references in the record area pointed to by <paramref name="record_p"/>.
		/// </summary>
		/// <param name="record_p"></param>
		/// <remarks>
		/// This should only be used when the record is be reclaimed.
		/// </remarks>
		private void RemoveAllBlobReferencesForRecord(long record_p) {
			// NOTE: Does this need to be optimized?
			IArea record_area = store.GetArea(record_p);
			int reserved = record_area.ReadInt4();  // reserved
			// Look for any blob references input the row
			for (int i = 0; i < column_count; ++i) {
				int ctype = record_area.ReadInt4();
				int cell_offset = record_area.ReadInt4();
				if (ctype == 1) {
					// Type 1 is not a large object
				} else if (ctype == 2) {
					int cur_p = record_area.Position;
					record_area.Position = cell_offset + 4 + (column_count * 8);
					int btype = record_area.ReadInt4();
					record_area.ReadInt4();    // (reserved)
					if (btype == 0) {
						long blob_ref_id = record_area.ReadInt8();
						// Release this reference
						blob_store.ReleaseReference(blob_ref_id);
					}
					// Revert the area pointer
					record_area.Position = cur_p;
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
			lock (list_structure) {
				long elements = list_structure.AddressableNodeCount;
				for (long i = 0; i < elements; ++i) {
					IArea a = list_structure.PositionOnNode(i);
					int status = a.ReadInt4();
					// Is the record not deleted?
					if ((status & 0x020000) == 0) {
						// Get the record pointer
						long record_p = a.ReadInt8();
						RemoveAllBlobReferencesForRecord(record_p);
					}
				}
			}

		}

		// ---------- Diagnostic and repair ----------

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
			lock (list_structure) {

				// The list of pointers to areas (as Long).
				ArrayList used_areas = new ArrayList();

				// Add the header_p pointer
				used_areas.Add(header_area.Id);

				header_area.Position = 16;
				// Add the DataTableDef and DataIndexSetDef objects
				used_areas.Add(header_area.ReadInt8());
				used_areas.Add(header_area.ReadInt8());

				// Add all the used areas input the list_structure itself.
				list_structure.AddAllAreasUsed(used_areas);

				// Adds all the user areas input the index store.
				index_store.addAllAreasUsed(used_areas);

				// Search the list structure for all areas
				long elements = list_structure.AddressableNodeCount;
				for (long i = 0; i < elements; ++i) {
					IArea a = list_structure.PositionOnNode(i);
					int status = a.ReadInt4();
					if ((status & 0x020000) == 0) {
						long pointer = a.ReadInt8();
						//          Console.Out.WriteLine("Not deleted = " + pointer);
						// Record is not deleted,
						used_areas.Add(pointer);
					}
				}

				// Following depends on store implementation
				if (store is AbstractStore) {
					AbstractStore a_store = (AbstractStore)store;
					ArrayList leaked_areas = a_store.FindAllocatedAreasNotIn(used_areas);
					if (leaked_areas.Count == 0) {
						Debug.Write(DebugLevel.Information, this, "No leaked areas.");
					} else {
						Debug.Write(DebugLevel.Information, this, "There were " +
									  leaked_areas.Count + " leaked areas found.");
						for (int n = 0; n < leaked_areas.Count; ++n) {
							long area_pointer = (long)leaked_areas[n];
							store.DeleteArea(area_pointer);
						}
						Debug.Write(DebugLevel.Information, this,
									  "Leaked areas successfully freed.");
					}
				}

			}

		}

		/// <summary>
		/// Performs a complete check and repair of the table.
		/// </summary>
		/// <param name="file_name"></param>
		/// <param name="terminal">An implementation of the user interface 
		/// <see cref="IUserTerminal"/ >that is used to ask any questions 
		/// and output the results of the check.</param>
		/// <remarks>
		/// The table must not have been opened before this method is called.  
		/// </remarks>
		public void CheckAndRepair(String file_name, IUserTerminal terminal) {
			this.file_name = file_name;

			terminal.WriteLine("+ Repairing V2MasterTableDataSource " + file_name);

			store = StoreSystem.OpenStore(file_name);
			// If AbstractStore then fix
			if (store is AbstractStore) {
				((AbstractStore)store).OpenScanAndFix(terminal);
			}

			// Setup the list structure
			list_structure = new FixedRecordList(store, 12);

			try {
				// Read and setup the pointers
				ReadStoreHeaders();
				// Set the column count
				column_count = table_def.ColumnCount;
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
			IList all_areas = store.GetAllAreas();
			// The list of all records generated when we check each record
			ArrayList all_records = new ArrayList();

			// Look up each record and check it's intact,  Any records that are deleted
			// are added to the delete chain.
			first_delete_chain_record = -1;
			int record_count = 0;
			int free_count = 0;
			int sz = RawRowCount;
			for (int i = sz - 1; i >= 0; --i) {
				bool record_valid = CheckAndRepairRecord(i, all_areas, terminal);
				if (record_valid) {
					all_records.Add(i);
					++record_count;
				} else {
					++free_count;
				}
			}
			// Set the reserved area
			list_structure.WriteReservedLong(first_delete_chain_record);

			terminal.Write("* Record count = " + record_count);
			terminal.WriteLine(" Free count = " + free_count);

			// Check indexes
			terminal.WriteLine("- Rebuilding all table index information.");

			int index_count = table_def.ColumnCount + 1;
			for (int i = 0; i < index_count; ++i) {
				index_store.CommitDropIndex(i);
			}
			//    store.flush();
			BuildIndexes();

			terminal.WriteLine("- Table check complete.");
			//    // Flush any changes
			//    store.flush();

		}

		/// <summary>
		/// Checks and repairs a record if it requires repairing.
		/// </summary>
		/// <param name="row_index"></param>
		/// <param name="all_areas"></param>
		/// <param name="terminal"></param>
		/// <remarks>
		/// Returns true if the record is valid, or false otherwise (record is/was deleted).
		/// </remarks>
		/// <returns></returns>
		private bool CheckAndRepairRecord(int row_index, ICollection all_areas, IUserTerminal terminal) {
			lock (list_structure) {
				// Position input the list structure
				IMutableArea block_area = list_structure.PositionOnNode(row_index);
				int p = block_area.Position;
				int status = block_area.ReadInt4();
				// If it is not deleted,
				if ((status & 0x020000) == 0) {
					long record_p = block_area.ReadInt8();
					//        Console.Out.WriteLine("row_index = " + row_index + " record_p = " + record_p);
					// Is this pointer valid?
					//TODO: check this...
					int i = new ArrayList(all_areas).BinarySearch(record_p);
					if (i >= 0) {
						// Pointer is valid input the store,
						// Try reading from column 0
						try {
							InternalGetCellContents(0, row_index);
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
					terminal.WriteLine("+ Record area not valid: row = " + row_index +
									 " pointer = " + record_p);
					terminal.WriteLine("+ Deleting record.");
				}
				// Put this record input the delete chain
				block_area.Position = p;
				block_area.WriteInt4(0x020000);
				block_area.WriteInt8(first_delete_chain_record);
				block_area.CheckOut();
				first_delete_chain_record = row_index;

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
				list_structure.IncreaseSize();
				// The start record of the new size
				int new_block_number = list_structure.ListBlockCount - 1;
				long start_index =
							   list_structure.ListBlockFirstPosition(new_block_number);
				long size_of_block = list_structure.ListBlockNodeCount(new_block_number);

				// The IArea object for the new position
				IMutableArea a = list_structure.PositionOnNode(start_index);

				// Set the rest of the block as deleted records
				for (long n = 0; n < size_of_block - 1; ++n) {
					a.WriteInt4(0x020000);
					a.WriteInt8(start_index + n + 1);
				}
				// The last block is end of delete chain.
				a.WriteInt4(0x020000);
				a.WriteInt8(first_delete_chain_record);
				a.CheckOut();
				// And set the new delete chain
				first_delete_chain_record = start_index;
				// Set the reserved area
				list_structure.WriteReservedLong(first_delete_chain_record);

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Adds a record to the given position in the fixed structure.
		/// </summary>
		/// <param name="index"></param>
		/// <param name="record_p"></param>
		/// <remarks>
		/// If the place is already used by a record then an exception is thrown, 
		/// otherwise the record is set.
		/// </remarks>
		/// <returns></returns>
		private long AddToRecordList(long index, long record_p) {
			lock (list_structure) {
				if (has_shutdown) {
					throw new IOException("IO operation while VM shutting down.");
				}

				long addr_count = list_structure.AddressableNodeCount;
				// First make sure there are enough nodes to accomodate this entry,
				while (index >= addr_count) {
					GrowListStructure();
					addr_count = list_structure.AddressableNodeCount;
				}

				// Remove this from the delete chain by searching for the index input the
				// delete chain.
				long prev = -1;
				long chain = first_delete_chain_record;
				while (chain != -1 && chain != index) {
					IArea a1 = list_structure.PositionOnNode(chain);
					if (a1.ReadInt4() == 0x020000) {
						prev = chain;
						chain = a1.ReadInt8();
					} else {
						throw new IOException("Not deleted record is input delete chain!");
					}
				}
				// Wasn't found
				if (chain == -1) {
					throw new IOException(
								 "Unable to add record because index is not available.");
				}
				// Read the next entry input the delete chain.
				IArea a = list_structure.PositionOnNode(chain);
				if (a.ReadInt4() != 0x020000) {
					throw new IOException("Not deleted record is input delete chain!");
				}
				long next_p = a.ReadInt8();

				try {
					store.LockForWrite();

					// If prev == -1 then first_delete_chain_record points to this record
					if (prev == -1) {
						first_delete_chain_record = next_p;
						list_structure.WriteReservedLong(first_delete_chain_record);
					} else {
						// Otherwise we need to set the previous node to point to the next node
						IMutableArea ma1 = list_structure.PositionOnNode(prev);
						ma1.WriteInt4(0x020000);
						ma1.WriteInt8(next_p);
						ma1.CheckOut();
					}

					// Finally set the record_p
					IMutableArea ma = list_structure.PositionOnNode(index);
					ma.WriteInt4(0);
					ma.WriteInt8(record_p);
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
		/// <param name="record_p"></param>
		/// <remarks>
		/// This may expand the record space as necessary if there are no free 
		/// record slots to use.
		/// </remarks>
		/// <returns></returns>
		private long AddToRecordList(long record_p) {
			lock (list_structure) {
				if (has_shutdown) {
					throw new IOException("IO operation while VM shutting down.");
				}

				// If there are no free deleted records input the delete chain,
				if (first_delete_chain_record == -1) {
					// Grow the fixed structure to allow more nodes,
					GrowListStructure();
				}

				// Pull free block from the delete chain and recycle it.
				long recycled_record = first_delete_chain_record;
				IMutableArea block = list_structure.PositionOnNode(recycled_record);
				int rec_pos = block.Position;
				// Status of the recycled block
				int status = block.ReadInt4();
				if ((status & 0x020000) == 0) {
					throw new ApplicationException("Assertion failed: record is not deleted.  " +
									"status = " + status + ", rec_pos = " + rec_pos);
				}
				// The pointer to the next input the chain.
				long next_chain = block.ReadInt8();
				first_delete_chain_record = next_chain;

				try {

					store.LockForWrite();

					// Update the first_delete_chain_record field input the header
					list_structure.WriteReservedLong(first_delete_chain_record);
					// Update the block
					block.Position = rec_pos;
					block.WriteInt4(0);
					block.WriteInt8(record_p);
					block.CheckOut();

				} finally {
					store.UnlockForWrite();
				}

				return recycled_record;
			}
		}


		// ---------- Implemented from AbstractMasterTableDataSource ----------

		internal override string SourceIdentity {
			get { return file_name; }
		}


		internal override int WriteRecordType(int row_index, int row_state) {
			lock (list_structure) {
				if (has_shutdown) {
					throw new IOException("IO operation while VM shutting down.");
				}

				// Find the record entry input the block list.
				IMutableArea block_area = list_structure.PositionOnNode(row_index);
				int pos = block_area.Position;
				// Get the status.
				int old_status = block_area.ReadInt4();
				int mod_status = (int)(old_status & 0x0FFFF0000) | (row_state & 0x0FFFF);

				// Write the new status
				try {

					store.LockForWrite();

					block_area.Position = pos;
					block_area.WriteInt4(mod_status);
					block_area.CheckOut();

				} finally {
					store.UnlockForWrite();
				}

				return old_status & 0x0FFFF;
			}
		}


		internal override int ReadRecordType(int row_index) {
			lock (list_structure) {
				// Find the record entry input the block list.
				IArea block_area = list_structure.PositionOnNode(row_index);
				// Get the status.
				return block_area.ReadInt4() & 0x0FFFF;
			}
		}


		internal override bool RecordDeleted(int row_index) {
			lock (list_structure) {
				// Find the record entry input the block list.
				IArea block_area = list_structure.PositionOnNode(row_index);
				// If the deleted bit set for the record
				return (block_area.ReadInt4() & 0x020000) != 0;
			}
		}


		internal override int RawRowCount {
			get {
				lock (list_structure) {
					long total = list_structure.AddressableNodeCount;
					// 32-bit row limitation here - we should return a long.
					return (int) total;
				}
			}
		}


		internal override void InternalDeleteRow(int row_index) {
			long record_p;
			lock (list_structure) {
				if (has_shutdown) {
					throw new IOException("IO operation while VM shutting down.");
				}

				// Find the record entry input the block list.
				IMutableArea block_area = list_structure.PositionOnNode(row_index);
				int p = block_area.Position;
				int status = block_area.ReadInt4();
				// Check it is not already deleted
				if ((status & 0x020000) != 0) {
					throw new IOException("Record is already marked as deleted.");
				}
				record_p = block_area.ReadInt8();

				// Update the status record.
				try {
					store.LockForWrite();

					block_area.Position = p;
					block_area.WriteInt4(0x020000);
					block_area.WriteInt8(first_delete_chain_record);
					block_area.CheckOut();
					first_delete_chain_record = row_index;
					// Update the first_delete_chain_record field input the header
					list_structure.WriteReservedLong(first_delete_chain_record);

					// If the record contains any references to blobs, remove the reference
					// here.
					RemoveAllBlobReferencesForRecord(record_p);

					// Free the record from the store
					store.DeleteArea(record_p);

				} finally {
					store.UnlockForWrite();
				}

			}

		}


		internal override IIndexSet CreateIndexSet() {
			return index_store.GetSnapshotIndexSet();
		}


		internal override void CommitIndexSet(IIndexSet index_set) {
			index_store.CommitIndexSet(index_set);
			index_set.Dispose();
		}

		internal override int InternalAddRow(RowData data) {

			long row_number;
			int int_row_number;

			// Write the record to the store.
			lock (list_structure) {
				long record_p = WriteRecordToStore(data);
				// Now add this record into the record block list,
				row_number = AddToRecordList(record_p);
				int_row_number = (int)row_number;
			}

			// Update the cell cache as appropriate
			if (DATA_CELL_CACHING) {
				int row_cells = data.ColumnCount;
				for (int i = 0; i < row_cells; ++i) {
					// Put the row/column/TObject into the cache.
					cache.Set(table_id, int_row_number, i, data.GetCellData(i));
				}
			}

			// Return the record index of the new data input the table
			// NOTE: We are casting this from a long to int which means we are limited
			//   to ~2 billion record references.
			return (int)row_number;

		}


		internal override void checkForCleanup() {
			lock (this) {
				//    index_store.cleanUpEvent();
				gc.Collect(false);
			}
		}



		// ---- GetCellContents ----

		private static void SkipStream(Stream input, long amount) {
			long count = amount;
			long skipped = 0;

			while (skipped < amount) {
				long last_skipped = 0;
				if (input is InputStream) {
					InputStream inputStream = (InputStream) input;
					last_skipped = inputStream.Skip(count);
				} else {
					long pos = input.Position;
					last_skipped = (input.Seek(count, SeekOrigin.Current) - pos);
				}
				skipped += last_skipped;
				count -= last_skipped;
			}
		}


		//  private short s_run_total_hits = 0;
		private short s_run_file_hits = Int16.MaxValue;

		// ---- Optimization that saves some cycles -----

		internal override TObject InternalGetCellContents(int column, int row) {

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
			//      getSystem().stats().add(s_run_total_hits, total_hits_key);
			//      getSystem().stats().add(s_run_file_hits, file_hits_key);
			//      s_run_total_hits = 0;
			//      s_run_file_hits = 0;
			//    }

			//    // Increment the total hits counter
			//    ++s_run_total_hits;

			// First check if this is within the cache before we continue.
			TObject cell;
			if (DATA_CELL_CACHING) {
				cell = cache.Get(table_id, row, column);
				if (cell != null) {
					return cell;
				}
			}

			// We maintain a cache of byte[] arrays that contain the rows Read input
			// from the file.  If consequtive reads are made to the same row, then
			// this will cause lots of fast cache hits.

			long record_p = -1;
			try {
				lock (list_structure) {

					// Increment the file hits counter
					++s_run_file_hits;

					if (s_run_file_hits >= 100) {
						System.Stats.Add(s_run_file_hits, file_hits_key);
						s_run_file_hits = 0;
					}

					// Get the node for the record
					IArea list_block = list_structure.PositionOnNode(row);
					int status = list_block.ReadInt4();
					// Check it's not deleted
					if ((status & 0x020000) != 0) {
						throw new ApplicationException("Unable to Read deleted record.");
					}
					// Get the pointer to the record we are reading
					record_p = list_block.ReadInt8();

				}

				// Open a stream to the record
				BinaryReader din = GetBReader(store.GetAreaInputStream(record_p));

				SkipStream(din.BaseStream, 4 + (column * 8));
				int cell_type = din.ReadInt32();
				int cell_offset = din.ReadInt32();

				int cur_at = 8 + 4 + (column * 8);
				int be_at = 4 + (column_count * 8);
				int skip_amount = (be_at - cur_at) + cell_offset;

				SkipStream(din.BaseStream, skip_amount);

				Object ob;
				if (cell_type == 1) {
					// If standard object type
					ob = ObjectTransfer.ReadFrom(din);
				} else if (cell_type == 2) {
					// If reference to a blob input the BlobStore
					int f_type = din.ReadInt32();
					int f_reserved = din.ReadInt32();
					long ref_id = din.ReadInt64();
					if (f_type == 0) {
						// Resolve the reference
						ob = blob_store.GetLargeObject(ref_id);
					} else if (f_type == 1) {
						ob = null;
					} else {
						throw new Exception("Unknown blob type.");
					}
				} else {
					throw new Exception("Unrecognised cell type input data.");
				}

				// Get the TType for this column
				// NOTE: It's possible this call may need optimizing?
				TType ttype = DataTableDef[column].TType;
				// Wrap it around a TObject
				cell = new TObject(ttype, ob);

				// And close the reader.
				din.Close();

			} catch (IOException e) {
				Debug.WriteException(e);
				//      Console.Out.WriteLine("Pointer = " + row_pointer);
				throw new Exception("IOError getting cell at (" + column + ", " +
										   row + ") pointer = " + record_p + ".");
			}

			// And WriteByte input the cache and return it.
			if (DATA_CELL_CACHING) {
				cache.Set(table_id, row, column, cell);
			}

			return cell;

		}


		internal override long CurrentUniqueId {
			get {
				lock (list_structure) {
					return sequence_id - 1;
				}
			}
		}


		internal override long NextUniqueId {
			get {
				lock (list_structure) {
					long v = sequence_id;
					++sequence_id;
					if (has_shutdown) {
						throw new Exception("IO operation while VM shutting down.");
					}
					try {
						try {
							store.LockForWrite();
							header_area.Position = 4 + 4;
							header_area.WriteInt8(sequence_id);
							header_area.CheckOut();
						} finally {
							store.UnlockForWrite();
						}
					} catch (IOException e) {
						Debug.WriteException(e);
						throw new ApplicationException("IO Error: " + e.Message);
					}
					return v;
				}
			}
		}


		internal override void SetUniqueID(long value) {
			lock (list_structure) {
				sequence_id = value;
				if (has_shutdown) {
					throw new Exception("IO operation while VM shutting down.");
				}
				try {
					try {
						store.LockForWrite();
						header_area.Position = 4 + 4;
						header_area.WriteInt8(sequence_id);
						header_area.CheckOut();
					} finally {
						store.UnlockForWrite();
					}
				} catch (IOException e) {
					Debug.WriteException(e);
					throw new ApplicationException("IO Error: " + e.Message);
				}
			}
		}

		internal override void Dispose(bool pending_drop) {
			lock (this) {
				lock (list_structure) {
					if (!is_closed) {
						Close(pending_drop);
					}
				}
			}
		}

		internal override bool Drop() {
			lock (this) {
				lock (list_structure) {
					if (!is_closed) {
						Close(true);
					}

					bool b = StoreSystem.DeleteStore(store);
					if (b) {
						Debug.Write(DebugLevel.Message, this, "Dropped: " + SourceIdentity);
					}
					return b;

				}
			}
		}

		internal override void ShutdownHookCleanup() {
			//    try {
			lock (list_structure) {
				index_store.Close();
				//        store.synch();
				has_shutdown = true;
			}
			//    }
			//    catch (IOException e) {
			//      Debug.Write(DebugLevel.Error, this, "IO Error during shutdown hook.");
			//      Debug.WriteException(e);
			//    }
		}

		internal override bool Compact {
			get {
				// PENDING: We should perform some analysis on the data to decide if a
				//   compact is necessary or not.
				return true;
			}
		}


		/**
		 * For diagnostic.
		 */
		public override String ToString() {
			return "[V2MasterTableDataSource: " + file_name + "]";
		}
	}
}