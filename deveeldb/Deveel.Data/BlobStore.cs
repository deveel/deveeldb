//  
//  BlobStore.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.IO;
using System.Text;

using Deveel.Data.Store;
using Deveel.Data.Util;
using Deveel.Zip;

namespace Deveel.Data {
	/// <summary>
	/// A structure inside an <see cref="IArea"/> that maintains the storage of any 
	/// number of large binary objects.
	/// </summary>
	/// <remarks>
	/// A blob store allows for the easy allocation of areas for storing blob data 
	/// and for reading and writing blob information via <see cref="IBlobRef"/> objects.
	/// <para>
	/// A <see cref="BlobStore"/> can be broken down to the following simplistic functions;
	/// </para>
	/// <list type="number">
	/// <item>Allocation of an area to store a new blob.</item>
	/// <item>Reading the information in a IBlob given a IBlob reference identifier.</item>
	/// <item>Reference counting to a particular IBlob.</item>
	/// <item>Cleaning up a IBlob when no static references are left.</item>
	/// </list>
	/// </remarks>
	sealed class BlobStore : IBlobStore {

		/// <summary>
		/// The magic value for fixed record list structures.
		/// </summary>
		private const int MAGIC = 0x012BC53A9;

		/// <summary>
		/// The outer Store object that is to contain the blob store.
		/// </summary>
		private readonly IStore store;

		/// <summary>
		/// The <see cref="FixedRecordList"/> structure that maintains a list of fixed 
		/// size records for blob reference counting.
		/// </summary>
		private readonly FixedRecordList fixed_list;

		/// <summary>
		/// The first delete chain element.
		/// </summary>
		private long first_delete_chain_record;


		/// <summary>
		/// Constructs the <see cref="BlobStore"/> on the given IArea object. 
		/// </summary>
		/// <param name="store"></param>
		internal BlobStore(IStore store) {
			this.store = store;
			fixed_list = new FixedRecordList(store, 24);
		}


		/// <summary>
		/// Creates the blob store and returns a pointer in the store to 
		/// the header information.
		/// </summary>
		/// <remarks>
		/// This value is later used to initialize the store.
		/// </remarks>
		public long Create() {
			// Init the fixed record list area.
			// The fixed list entries are formatted as follows;
			//  ( status (int), reference_count (int),
			//    blob_size (long), blob_pointer (long) )
			long fixed_list_p = fixed_list.Create();

			// Delete chain is empty when we start
			first_delete_chain_record = -1;
			fixed_list.WriteReservedLong(-1);

			// Allocate a small header that contains the MAGIC, and the pointer to the
			// fixed list structure.
			IAreaWriter blob_store_header = store.CreateArea(32);
			long blob_store_p = blob_store_header.Id;
			// Write the blob store header information
			// The magic
			blob_store_header.WriteInt4(MAGIC);
			// The version
			blob_store_header.WriteInt4(1);
			// The pointer to the fixed list area
			blob_store_header.WriteInt8(fixed_list_p);
			// And finish
			blob_store_header.Finish();

			// Return the pointer to the blob store header
			return blob_store_p;
		}

		/// <summary>
		/// Initializes the blob store given a pointer to the blob store pointer 
		/// header (the value previously returned by the <see cref="Create"/> method).
		/// </summary>
		public void Init(long blob_store_p) {
			// Get the header area
			IArea blob_store_header = store.GetArea(blob_store_p);
			blob_store_header.Position = 0;
			// Read the magic
			int magic = blob_store_header.ReadInt4();
			int version = blob_store_header.ReadInt4();
			if (magic != MAGIC) {
				throw new IOException("MAGIC value for BlobStore is not correct.");
			}
			if (version != 1) {
				throw new IOException("version number for BlobStore is not correct.");
			}

			// Read the pointer to the fixed area
			long fixed_list_p = blob_store_header.ReadInt8();
			// Init the FixedRecordList area
			fixed_list.Init(fixed_list_p);

			// Set the delete chain
			first_delete_chain_record = fixed_list.ReadReservedLong();
		}


		/// <summary>
		/// Simple structure used when copying blob information.
		/// </summary>
		private sealed class CopyBlobInfo {
			public int ref_count;
			public long size;
			public long ob_p;
		};

		/// <summary>
		/// Copies all the blob data from the given <see cref="BlobStore"/> into this blob store.
		/// </summary>
		/// <param name="store_system"></param>
		/// <param name="src_blob_store"></param>
		/// <remarks>
		/// Any blob information that already exists within this <see cref="BlobStore"/> 
		/// is deleted. We assume this method is called after the blob store is created 
		/// or initialized.
		/// </remarks>
		public void CopyFrom(IStoreSystem store_system, BlobStore src_blob_store) {
			FixedRecordList src_fixed_list = src_blob_store.fixed_list;
			long node_count;
			lock (src_fixed_list) {
				node_count = src_fixed_list.AddressableNodeCount;
			}

			lock (fixed_list) {

				// Make sure our fixed_list is big enough to accomodate the copied list,
				while (fixed_list.AddressableNodeCount < node_count) {
					fixed_list.IncreaseSize();
				}

				// We rearrange the delete chain
				long last_deleted = -1;

				// We copy blobs in groups no larger than 1024 Blobs
				const int BLOCK_WRITE_COUNT = 1024;

				int max_to_read = (int)System.Math.Min(BLOCK_WRITE_COUNT, node_count);
				long p = 0;

				while (max_to_read > 0) {
					// (CopyBlboInfo)
					ArrayList src_copy_list = new ArrayList();

					lock (src_fixed_list) {
						for (int i = 0; i < max_to_read; ++i) {
							IArea a = src_fixed_list.PositionOnNode(p + i);
							int status = a.ReadInt4();
							// If record is not deleted
							if (status != 0x020000) {
								CopyBlobInfo info = new CopyBlobInfo();
								info.ref_count = a.ReadInt4();
								info.size = a.ReadInt8();
								info.ob_p = a.ReadInt8();
								src_copy_list.Add(info);
							} else {
								src_copy_list.Add(null);
							}
						}
					}

					try {
						store.LockForWrite();

						// We now should have a list of all records from the src to copy,
						int sz = src_copy_list.Count;
						for (int i = 0; i < sz; ++i) {
							CopyBlobInfo info = (CopyBlobInfo)src_copy_list[i];
							IMutableArea a = fixed_list.PositionOnNode(p + i);
							// Either set a deleted entry or set the entry with a copied blob.
							if (info == null) {
								a.WriteInt4(0x020000);
								a.WriteInt4(0);
								a.WriteInt8(-1);
								a.WriteInt8(last_deleted);
								a.CheckOut();
								last_deleted = p + i;
							} else {
								// Get the IArea containing the blob header data in the source
								// store
								IArea src_blob_header = src_blob_store.store.GetArea(info.ob_p);
								// Read the information from the header,
								int res = src_blob_header.ReadInt4();
								int type = src_blob_header.ReadInt4();
								long total_block_size = src_blob_header.ReadInt8();
								long total_block_pages = src_blob_header.ReadInt8();

								// Allocate a new header
								IAreaWriter dst_blob_header = store.CreateArea(
									4 + 4 + 8 + 8 + (total_block_pages * 8));
								long new_ob_header_p = dst_blob_header.Id;
								// Copy information into the header
								dst_blob_header.WriteInt4(res);
								dst_blob_header.WriteInt4(type);
								dst_blob_header.WriteInt8(total_block_size);
								dst_blob_header.WriteInt8(total_block_pages);

								// Allocate and copy each page,
								for (int n = 0; n < total_block_pages; ++n) {
									// Get the block information
									long block_p = src_blob_header.ReadInt8();
									IArea src_block = src_blob_store.store.GetArea(block_p);
									int block_type = src_block.ReadInt4();
									int block_size = src_block.ReadInt4();
									// Copy a new block,
									int new_block_size = block_size + 4 + 4;
									IAreaWriter dst_block_p = store.CreateArea(new_block_size);
									long new_block_p = dst_block_p.Id;
									src_block.Position = 0;
									src_block.CopyTo(dst_block_p, new_block_size);
									// And finish
									dst_block_p.Finish();
									// Write the new header
									dst_blob_header.WriteInt8(new_block_p);
								}

								// And finish 'dst_blob_header'
								dst_blob_header.Finish();

								// Set up the data in the fixed list
								a.WriteInt4(1);
								// Note all the blobs are written with 0 reference count.
								a.WriteInt4(0);
								a.WriteInt8(info.size);
								a.WriteInt8(new_ob_header_p);
								// Check out the changes
								a.CheckOut();
							}
						}

					} finally {
						store.UnlockForWrite();
					}

					node_count -= max_to_read;
					p += max_to_read;
					max_to_read = (int)System.Math.Min(BLOCK_WRITE_COUNT, node_count);

					// Set a checkpoint in the destination store system so we Write out
					// all pending changes from the log
					store_system.SetCheckPoint();

				}

				// Set the delete chain
				first_delete_chain_record = last_deleted;
				fixed_list.WriteReservedLong(last_deleted);

			} // Lock (fixed_list)

		}

		/// <summary>
		/// Convenience method that converts the given string into a <see cref="IClobRef"/> 
		/// object and pushes it into the given <see cref="BlobStore"/> object.
		/// </summary>
		public IClobRef WriteString(String str) {
			const int BUF_SIZE = 64 * 1024;

			int size = str.Length;

			byte type = 4;
			// Enable compression (ISSUE: Should this be enabled by default?)
			type = (byte)(type | 0x010);

			//TODO: check the size and encoding of a char...
			IClobRef reference = (IClobRef)AllocateLargeObject(type, size * 2);
			byte[] buf = new byte[BUF_SIZE];
			long p = 0;
			int str_i = 0;
			while (size > 0) {
				int to_write = System.Math.Min(BUF_SIZE / 2, size);
				int buf_i = 0;
				for (int i = 0; i < to_write; ++i) {
					char c = str[str_i];
					buf[buf_i] = (byte)(c >> 8);
					++buf_i;
					buf[buf_i] = (byte)c;
					++buf_i;
					++str_i;
				}
				reference.Write(p, buf, buf_i);
				size -= to_write;
				p += to_write * 2;
			}

			reference.Complete();

			return reference;
		}

		/// <summary>
		/// Convenience method that converts the given <see cref="ByteLongObject"/> 
		/// into a <see cref="IBlobRef"/> object and pushes it into the given 
		/// <see cref="IBlobStore"/> object.
		/// </summary>
		/// <param name="blob"></param>
		/// <returns></returns>
		public IBlobRef WriteByteLongObject(ByteLongObject blob) {
			const int BUF_SIZE = 64 * 1024;

			byte[] src_buf = blob.ToArray();
			int size = src_buf.Length;
			IBlobRef reference = (IBlobRef)AllocateLargeObject((byte)2, size);

			byte[] copy_buf = new byte[BUF_SIZE];
			int offset = 0;
			int to_write = System.Math.Min(BUF_SIZE, size);

			while (to_write > 0) {
				Array.Copy(src_buf, offset, copy_buf, 0, to_write);
				reference.Write(offset, copy_buf, to_write);

				offset += to_write;
				to_write = System.Math.Min(BUF_SIZE, (size - offset));
			}

			reference.Complete();

			return reference;
		}

		/// <summary>
		/// Finds a free place to add a record and returns an index to the record here.
		/// </summary>
		/// <param name="record_p"></param>
		/// <remarks>
		/// This may expand the record space as necessary if there are no free record
		/// slots to use.
		/// </remarks>
		/// <returns></returns>
		private long AddToRecordList(long record_p) {

			lock (fixed_list) {
				// If there is no free deleted records in the delete chain,
				if (first_delete_chain_record == -1) {

					// Increase the size of the list structure.
					fixed_list.IncreaseSize();
					// The start record of the new size
					int new_block_number = fixed_list.ListBlockCount - 1;
					long start_index = fixed_list.ListBlockFirstPosition(new_block_number);
					long size_of_block = fixed_list.ListBlockNodeCount(new_block_number);
					// The IArea object for the new position
					IMutableArea a = fixed_list.PositionOnNode(start_index);

					a.WriteInt4(0);
					a.WriteInt4(0);
					a.WriteInt8(-1);  // Initially unknown size
					a.WriteInt8(record_p);
					// Set the rest of the block as deleted records
					for (long n = 1; n < size_of_block - 1; ++n) {
						a.WriteInt4(0x020000);
						a.WriteInt4(0);
						a.WriteInt8(-1);
						a.WriteInt8(start_index + n + 1);
					}
					// The last block is end of delete chain.
					a.WriteInt4(0x020000);
					a.WriteInt4(0);
					a.WriteInt8(-1);
					a.WriteInt8(-1);
					// Check out the changes.
					a.CheckOut();
					// And set the new delete chain
					first_delete_chain_record = start_index + 1;
					// Set the reserved area
					fixed_list.WriteReservedLong(first_delete_chain_record);
					//        // Flush the changes to the store
					//        store.flush();

					// Return pointer to the record we just added.
					return start_index;

				} else {

					// Pull free block from the delete chain and recycle it.
					long recycled_record = first_delete_chain_record;
					IMutableArea block = fixed_list.PositionOnNode(recycled_record);
					int rec_pos = block.Position;
					// Status of the recycled block
					int status = block.ReadInt4();
					if ((status & 0x020000) == 0) {
						throw new ApplicationException("Assertion failed: record is not deleted!");
					}
					// Reference count (currently unused in delete chains).
					block.ReadInt4();
					// The size (should be -1);
					block.ReadInt8();
					// The pointer to the next in the chain.
					long next_chain = block.ReadInt8();
					first_delete_chain_record = next_chain;
					// Update the first_delete_chain_record field in the header
					fixed_list.WriteReservedLong(first_delete_chain_record);
					// Update the block
					block.Position = rec_pos;
					block.WriteInt4(0);
					block.WriteInt4(0);
					block.WriteInt8(-1);    // Initially unknown size
					block.WriteInt8(record_p);
					// Check out the changes
					block.CheckOut();

					return recycled_record;
				}
			}
		}

		/// <summary>
		/// Allocates an area in the store for a large binary object to be stored.
		/// </summary>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// After the blob area is allocated the blob may be written. This returns 
		/// a <see cref="IBlobRef"/> object for future access to the blob.
		/// <para>
		/// A newly allocated blob is Read and Write enabled.  A call to the 
		/// <see cref="CompleteBlob"/> method must be called to finalize the blob at 
		/// which point the blob becomes a static read-only object.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IRef AllocateLargeObject(byte type, long size) {
			if (size < 0)
				throw new IOException("Negative blob size not allowed.");

			try {
				store.LockForWrite();

				// Allocate the area (plus header area) for storing the blob pages
				long page_count = ((size - 1) / (64 * 1024)) + 1;
				IAreaWriter blob_area = store.CreateArea((page_count * 8) + 24);
				long blob_p = blob_area.Id;
				// Set up the area header
				blob_area.WriteInt4(0);           // Reserved for future
				blob_area.WriteInt4(type);
				blob_area.WriteInt8(size);
				blob_area.WriteInt8(page_count);
				// Initialize the empty blob area
				for (long i = 0; i < page_count; ++i) {
					blob_area.WriteInt8(-1);
				}
				// And finish
				blob_area.Finish();

				// Update the fixed_list and return the record number for this blob
				long reference_id = AddToRecordList(blob_p);
				byte st_type = (byte)(type & 0x0F);
				if (st_type == 2) {
					// Create a IBlobRef implementation that can access this blob
					return new BlobRefImpl(this, reference_id, type, size, true);
				} else if (st_type == 3) {
					return new ClobRefImpl(this, reference_id, type, size, true);
				} else if (st_type == 4) {
					return new ClobRefImpl(this, reference_id, type, size, true);
				} else {
					throw new IOException("Unknown large object type");
				}

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// Returns a <see cref="IRef"/> object that allows read-only 
		/// access to a large object in this blob store.
		/// </summary>
		/// <param name="reference_id"></param>
		/// <returns></returns>
		public IRef GetLargeObject(long reference_id) {
			long blob_p;
			long size;
			lock (fixed_list) {

				// Assert that the blob reference id given is a valid range
				if (reference_id < 0 ||
				    reference_id >= fixed_list.AddressableNodeCount) {
					throw new IOException("reference_id is out of range.");
				}

				// Position on this record
				IArea block = fixed_list.PositionOnNode(reference_id);
				// Read the information in the fixed record
				int status = block.ReadInt4();
				// Assert that the status is not deleted
				if ((status & 0x020000) != 0) {
					throw new ApplicationException("Assertion failed: record is deleted!");
				}
				// Get the reference count
				int reference_count = block.ReadInt4();
				// Get the total size of the blob
				size = block.ReadInt8();
				// Get the blob pointer
				blob_p = block.ReadInt8();

			}

			IArea blob_area = store.GetArea(blob_p);
			blob_area.Position = 0;
			blob_area.ReadInt4();  // (reserved)
			// Read the type
			byte type = (byte)blob_area.ReadInt4();
			// The size of the block
			long block_size = blob_area.ReadInt8();
			// The number of pages in the blob
			long page_count = blob_area.ReadInt8();

			if (type == (byte)2) {
				// Create a new IBlobRef object.
				return new BlobRefImpl(this, reference_id, type, size, false);
			} else {
				// Create a new IClobRef object.
				return new ClobRefImpl(this, reference_id, type, size, false);
			}
		}

		/// <summary>
		/// Call this to complete a blob in the store after a blob has been 
		/// completely written.
		/// </summary>
		/// <param name="reference"></param>
		/// <remarks>
		/// Only <see cref="IBlobRef"/> implementations returned by the 
		/// <see cref="AllocateLargeObject"/> method are accepted.
		/// </remarks>
		private void CompleteBlob(AbstractRef reference) {
			// Assert that the IBlobRef is open and allocated
			reference.AssertIsOpen();
			// Get the blob reference id (reference to the fixed record list).
			long blob_reference_id = reference.Id;

			lock (fixed_list) {

				// Update the record in the fixed list.
				IMutableArea block = fixed_list.PositionOnNode(blob_reference_id);
				// Record the position
				int rec_pos = block.Position;
				// Read the information in the fixed record
				int status = block.ReadInt4();
				// Assert that the status is open
				if (status != 0) {
					throw new IOException("Assertion failed: record is not open.");
				}
				int reference_count = block.ReadInt4();
				long size = block.ReadInt8();
				long page_count = block.ReadInt8();

				try {
					store.LockForWrite();

					// Set the fixed blob record as complete.
					block.Position = rec_pos;
					// Write the new status
					block.WriteInt4(1);
					// Write the reference count
					block.WriteInt4(0);
					// Write the completed size
					block.WriteInt8(reference.RawSize);
					// Write the pointer
					block.WriteInt8(page_count);
					// Check out the change
					block.CheckOut();

				} finally {
					store.UnlockForWrite();
				}

			}
			// Now the blob has been finalized so change the state of the IBlobRef
			// object.
			reference.Close();

		}

		public void EstablishReference(long blob_reference_id) {
			try {
				lock (fixed_list) {
					// Update the record in the fixed list.
					IMutableArea block = fixed_list.PositionOnNode(blob_reference_id);
					// Record the position
					int rec_pos = block.Position;
					// Read the information in the fixed record
					int status = block.ReadInt4();
					// Assert that the status is static
					if (status != 1) {
						throw new Exception("Assertion failed: record is not static.");
					}
					int reference_count = block.ReadInt4();

					// Set the fixed blob record as complete.
					block.Position = rec_pos + 4;
					// Write the reference count + 1
					block.WriteInt4(reference_count + 1);
					// Check out the change
					block.CheckOut();
				}
				//      // Flush all changes to the store.
				//      store.flush();
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		public void ReleaseReference(long blob_reference_id) {
			try {
				lock (fixed_list) {
					// Update the record in the fixed list.
					IMutableArea block = fixed_list.PositionOnNode(blob_reference_id);
					// Record the position
					int rec_pos = block.Position;
					// Read the information in the fixed record
					int status = block.ReadInt4();
					// Assert that the status is static
					if (status != 1) {
						throw new Exception("Assertion failed: " +
						                    "Record is not static (status = " + status + ")");
					}
					int reference_count = block.ReadInt4();
					if (reference_count == 0) {
						throw new Exception("Releasing when IBlob reference counter is at 0.");
					}

					long object_size = block.ReadInt8();
					long object_p = block.ReadInt8();

					// If reference count == 0 then we need to free all the resources
					// associated with this IBlob in the blob store.
					if ((reference_count - 1) == 0) {
						// Free the resources associated with this object.
						IArea blob_area = store.GetArea(object_p);
						blob_area.ReadInt4();
						byte type = (byte)blob_area.ReadInt4();
						long total_size = blob_area.ReadInt8();
						long page_count = blob_area.ReadInt8();
						// Free all of the pages in this blob.
						for (long i = 0; i < page_count; ++i) {
							long page_p = blob_area.ReadInt8();
							if (page_p > 0) {
								store.DeleteArea(page_p);
							}
						}
						// Free the blob area object itself.
						store.DeleteArea(object_p);
						// Write out the blank record.
						block.Position = rec_pos;
						block.WriteInt4(0x020000);
						block.WriteInt4(0);
						block.WriteInt8(-1);
						block.WriteInt8(first_delete_chain_record);
						// CHeck out these changes
						block.CheckOut();
						first_delete_chain_record = blob_reference_id;
						// Update the first_delete_chain_record field in the header
						fixed_list.WriteReservedLong(first_delete_chain_record);
					} else {
						// Simply decrement the reference counter for this record.
						block.Position = rec_pos + 4;
						// Write the reference count - 1
						block.WriteInt4(reference_count - 1);
						// Check out this change
						block.CheckOut();
					}

				}
				//      // Flush all changes to the store.
				//      store.flush();
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}


		/// <summary>
		/// Reads a section of the blob referenced by the given id, offset and length
		/// into the byte array.
		/// </summary>
		/// <param name="reference_id"></param>
		/// <param name="offset"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="length"></param>
		private void ReadBlobByteArray(long reference_id, long offset, byte[] buf, int off, int length) {

			// ASSERT: Read and Write position must be 64K aligned.
			if (offset % (64 * 1024) != 0) {
				throw new Exception("Assert failed: offset is not 64k aligned.");
			}
			// ASSERT: Length is less than or equal to 64K
			if (length > (64 * 1024)) {
				throw new Exception("Assert failed: length is greater than 64K.");
			}

			int status;
			int reference_count;
			long size;
			long blob_p;

			lock (fixed_list) {

				// Assert that the blob reference id given is a valid range
				if (reference_id < 0 ||
				    reference_id >= fixed_list.AddressableNodeCount) {
					throw new IOException("blob_reference_id is out of range.");
				}

				// Position on this record
				IArea block = fixed_list.PositionOnNode(reference_id);
				// Read the information in the fixed record
				status = block.ReadInt4();
				// Assert that the status is not deleted
				if ((status & 0x020000) != 0) {
					throw new ApplicationException("Assertion failed: record is deleted!");
				}
				// Get the reference count
				reference_count = block.ReadInt4();
				// Get the total size of the blob
				size = block.ReadInt8();
				// Get the blob pointer
				blob_p = block.ReadInt8();

			}

			// Assert that the area being Read is within the bounds of the blob
			if (offset < 0 || offset + length > size) {
				throw new IOException("IBlob invalid Read.  offset = " + offset +
				                      ", length = " + length);
			}

			// Open an IArea into the blob
			IArea blob_area = store.GetArea(blob_p);
			blob_area.ReadInt4();
			byte type = (byte)blob_area.ReadInt4();

			// Convert to the page number
			long page_number = (offset / (64 * 1024));
			blob_area.Position = (int)((page_number * 8) + 24);
			long page_p = blob_area.ReadInt8();

			// Read the page
			IArea page_area = store.GetArea(page_p);
			page_area.Position = 0;
			int page_type = page_area.ReadInt4();
			int page_size = page_area.ReadInt4();
			if ((type & 0x010) != 0) {
				// The page is compressed
				byte[] page_buf = new byte[page_size];
				page_area.Read(page_buf, 0, page_size);
				Inflater inflater = new Inflater();
				inflater.SetInput(page_buf, 0, page_size);
				try {
					int result_length = inflater.Inflate(buf, off, length);
					if (result_length != length) {
						throw new Exception(
							"Assert failed: decompressed length is incorrect.");
					}
				} catch (FormatException e) {
					throw new IOException("ZIP Data Format Error: " + e.Message);
				}
				// inflater.End();
			} else {
				// The page is not compressed
				page_area.Read(buf, off, length);
			}

		}

		/// <summary>
		/// Writes a section of the blob referenced by the given id, offset and 
		/// length to the byte array.
		/// </summary>
		/// <param name="reference_id"></param>
		/// <param name="offset"></param>
		/// <param name="buf"></param>
		/// <param name="length"></param>
		/// <remarks>
		/// This does not perform any checks on whether we are allowed to write to this blob.
		/// </remarks>
		private void WriteBlobByteArray(long reference_id, long offset, byte[] buf, int length) {
			// ASSERT: Read and Write position must be 64K aligned.
			if (offset % (64 * 1024) != 0) {
				throw new Exception("Assert failed: offset is not 64k aligned.");
			}
			// ASSERT: Length is less than or equal to 64K
			if (length > (64 * 1024)) {
				throw new Exception("Assert failed: length is greater than 64K.");
			}

			int status;
			int reference_count;
			long size;
			long blob_p;

			lock (fixed_list) {

				// Assert that the blob reference id given is a valid range
				if (reference_id < 0 ||
				    reference_id >= fixed_list.AddressableNodeCount) {
					throw new IOException("blob_reference_id is out of range.");
				}

				// Position on this record
				IArea block = fixed_list.PositionOnNode(reference_id);
				// Read the information in the fixed record
				status = block.ReadInt4();
				// Assert that the status is not deleted
				if ((status & 0x020000) != 0) {
					throw new ApplicationException("Assertion failed: record is deleted!");
				}
				// Get the reference count
				reference_count = block.ReadInt4();
				// Get the total size of the blob
				size = block.ReadInt8();
				// Get the blob pointer
				blob_p = block.ReadInt8();

			}

			// Open an IArea into the blob
			IMutableArea blob_area = store.GetMutableArea(blob_p);
			blob_area.ReadInt4();
			byte type = (byte)blob_area.ReadInt4();
			size = blob_area.ReadInt8();

			// Assert that the area being Read is within the bounds of the blob
			if (offset < 0 || offset + length > size) {
				throw new IOException("IBlob invalid Write.  offset = " + offset +
				                      ", length = " + length + ", size = " + size);
			}

			// Convert to the page number
			long page_number = (offset / (64 * 1024));
			blob_area.Position = (int)((page_number * 8) + 24);
			long page_p = blob_area.ReadInt8();

			// Assert that 'page_p' is -1
			if (page_p != -1) {
				// This means we are trying to rewrite a page we've already written
				// before.
				throw new Exception("Assert failed: page_p is not -1");
			}

			// Is the compression bit set?
			byte[] to_write;
			int write_length;
			if ((type & 0x010) != 0) {
				// Yes, compression
				Deflater deflater = new Deflater();
				deflater.SetInput(buf, 0, length);
				deflater.Finish();
				to_write = new byte[65 * 1024];
				write_length = deflater.Deflate(to_write);
			} else {
				// No compression
				to_write = buf;
				write_length = length;
			}

			try {
				store.LockForWrite();

				// Allocate and Write the page.
				IAreaWriter page_area = store.CreateArea(write_length + 8);
				page_p = page_area.Id;
				page_area.WriteInt4(1);
				page_area.WriteInt4(write_length);
				page_area.Write(to_write, 0, write_length);
				// Finish this page
				page_area.Finish();

				// Update the page in the header.
				blob_area.Position = (int)((page_number * 8) + 24);
				blob_area.WriteInt8(page_p);
				// Check out this change.
				blob_area.CheckOut();

			} finally {
				store.UnlockForWrite();
			}

		}

		/// <summary>
		/// An <see cref="InputStream"/> implementation that reads from the underlying 
		/// blob data as fixed size pages.
		/// </summary>
		private class BLOBInputStream : PagedInputStream {

			const int B_SIZE = 64 * 1024;

			private readonly BlobStore store;
			private readonly long reference_id;

			public BLOBInputStream(BlobStore store, long reference_id, long size)
				: base(B_SIZE, size) {
				this.store = store;
				this.reference_id = reference_id;
			}

			protected override void ReadPageContent(byte[] buf, long pos, int length) {
				store.ReadBlobByteArray(reference_id, pos, buf, 0, length);
			}

		}

		/// <summary>
		/// An abstract implementation of a <see cref="IRef"/> object for referencing 
		/// large objects in this blob store.
		/// </summary>
		private class AbstractRef : IRef {
			internal readonly BlobStore store;
			/// <summary>
			/// The reference identifier.
			/// </summary>
			/// <remarks>
			/// This is a pointer into the fixed list structure.
			/// </remarks>
			protected readonly long reference_id;

			/// <summary>
			/// The total size of the large object in bytes.
			/// </summary>
			protected readonly long size;

			/// <summary>
			/// The type of large object.
			/// </summary>
			protected readonly byte type;

			/// <summary>
			/// Set to true if this large object is open for writing, otherwise the
			/// object is an immutable static object.
			/// </summary>
			private bool open_for_write;

			/// <summary>
			/// Constructs the <see cref="IRef"/> implementation.
			/// </summary>
			/// <param name="store"></param>
			/// <param name="reference_id"></param>
			/// <param name="type"></param>
			/// <param name="size"></param>
			/// <param name="open_for_write"></param>
			internal AbstractRef(BlobStore store, long reference_id, byte type, long size,
			                     bool open_for_write) {
				this.store = store;
				this.reference_id = reference_id;
				this.size = size;
				this.type = type;
				this.open_for_write = open_for_write;
			}

			/// <summary>
			/// Asserts that this blob is open for writing.
			/// </summary>
			internal void AssertIsOpen() {
				if (!open_for_write) {
					throw new ApplicationException("Large object reference is newly allocated.");
				}
			}

			public long RawSize {
				get { return size; }
			}

			/// <summary>
			/// Marks this large object as closed to write operations.
			/// </summary>
			internal void Close() {
				open_for_write = false;
			}

			public virtual int Length {
				get { return (int) size; }
			}

			public long Id {
				get { return reference_id; }
			}

			public byte Type {
				get { return type; }
			}

			public void Read(long offset, byte[] buf, int length) {
				// Reads the section of the blob into the given buffer byte array at the
				// given offset of the blob.
				store.ReadBlobByteArray(reference_id, offset, buf, 0, length);
			}

			public void Write(long offset, byte[] buf, int length) {
				if (open_for_write) {
					store.WriteBlobByteArray(reference_id, offset, buf, length);
				} else {
					throw new IOException("IBlob is Read-only.");
				}
			}

			public void Complete() {
				store.CompleteBlob(this);
			}

		}

		/// <summary>
		/// An implementation of <see cref="IClobRef"/> used to represent a reference 
		/// to a large character object inside this blob store.
		/// </summary>
		private class ClobRefImpl : AbstractRef, IClobRef {
			/// <summary>
			/// Constructs the IClobRef implementation.
			/// </summary>
			/// <param name="store"></param>
			/// <param name="reference_id"></param>
			/// <param name="type"></param>
			/// <param name="size"></param>
			/// <param name="open_for_write"></param>
			internal ClobRefImpl(BlobStore store, long reference_id, byte type, long size,
			                     bool open_for_write)
				: base(store, reference_id, type, size, open_for_write) {
			}

			public override int Length {
				get {
					byte st_type = (byte) (type & 0x0F);
					if (st_type == 3) {
						return (int) size;
					} else if (st_type == 4) {
						return (int) (size/2);
					} else {
						throw new Exception("Unknown type.");
					}
				}
			}

			public TextReader GetTextReader() {
				byte st_type = (byte)(type & 0x0F);
				//TODO: check this...
				if (st_type == 3) {
					return new StreamReader(new BLOBInputStream(store, reference_id, size), Encoding.ASCII);
				} else if (st_type == 4) {
					return new StreamReader(new BLOBInputStream(store, reference_id, size), Encoding.Unicode);
				} else {
					throw new Exception("Unknown type.");
				}
			}

			public override String ToString() {
				const int BUF_SIZE = 8192;
				TextReader r = GetTextReader();
				StringBuilder buf = new StringBuilder(Length);
				char[] c = new char[BUF_SIZE];
				try {
					while (true) {
						int has_read = r.Read(c, 0, BUF_SIZE);
						if (has_read == 0 || has_read == -1) {
							return buf.ToString();
						}
						buf.Append(c);
					}
				} catch (IOException e) {
					throw new Exception("IO Error: " + e.Message);
				}
			}

		}

		/// <summary>
		/// An implementation of <see cref="IBlobRef"/> used to represent a blob 
		/// reference inside this blob store.
		/// </summary>
		private class BlobRefImpl : AbstractRef, IBlobRef {
			/// <summary>
			/// Constructs the IBlobRef implementation.
			/// </summary>
			/// <param name="store"></param>
			/// <param name="reference_id"></param>
			/// <param name="type"></param>
			/// <param name="size"></param>
			/// <param name="open_for_write"></param>
			internal BlobRefImpl(BlobStore store, long reference_id, byte type, long size,
			                     bool open_for_write)
				: base(store, reference_id, type, size, open_for_write) {
			}

			public Stream GetInputStream() {
				return new BLOBInputStream(store, reference_id, size);
			}

		}
	}
}