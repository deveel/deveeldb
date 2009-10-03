// 
//  FixedRecordList.cs
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

namespace Deveel.Data.Store {
	/// <summary>
	/// A structure that provides a fast way to Read and Write fixed sized nodes in
	/// a <see cref="IStore"/> object.
	/// </summary>
	/// <remarks>
	/// This structure can locate a node in the list very quickly.  However, the
	/// structure can not be mutated.  For example, deleting node '4' will make the
	/// node available for recycling but will not shift any nodes after 4 in the
	/// list up by one.
	/// <para>
	/// Once a node is allocated from the list its position will not change.
	/// </para>
	/// <para>
	/// This structure does not provide versioning features.
	/// </para>
	/// <para>
	/// The structure is composed of two element types - the header and the list
	/// block elements.  The header is resembled by the following diagram:
	/// <code>
	///       LIST BLOCK HEADER
	///    +-------------------------------+
	///    | 4 MAGIC                       |
	///    | 4 list block count            |
	///    | 8 (reserved for delete chain) |
	///    | 8 pointer to list block 0     |
	///    | 8 pointer to list block 1     |
	///    .  ... etc ...                  .
	///    | 8 pointer to list block 63    |
	///    +-------------------------------+
	/// </code>
	/// </para>
	/// <para>
	/// The first list block element is 32 entries in size, the second list block is
	/// 64 entries in size, etc.  Each entry of the list block element is of fixed
	/// size.
	/// </para>
	/// <para>
	/// <b>Note:</b> This class is <b>not</b> thread safe.
	/// </para>
	/// </remarks>
	public class FixedRecordList {

		/// <summary>
		/// The magic value for fixed record list structures.
		/// </summary>
		private const int MAGIC = 0x087131AA;

		/// <summary>
		/// The backing Store object that persistantly stores the structure.
		/// </summary>
		private readonly IStore store;

		/// <summary>
		/// The fixed size of the elements in the list.
		/// </summary>
		private readonly int element_size;


		/// <summary>
		/// A pointer to the list header area.
		/// </summary>
		private long list_header_p;

		/// <summary>
		/// The header for the list blocks.
		/// </summary>
		private IMutableArea list_header_area;

		/// <summary>
		/// The number of blocks in the list block.
		/// </summary>
		private int list_block_count;

		// Pointers to the blocks in the list block.
		private readonly long[] list_block_element;
		private readonly IMutableArea[] list_block_area;


		/// <summary>
		/// Constructs the structure on the given <see cref="IStore"/> with the
		/// given fixed size for each element.
		/// </summary>
		/// <param name="store">The <see cref="IStore"/> object on which to store
		/// the structure element.</param>
		/// <param name="element_size">The fixed size for each element in the structure.</param>
		public FixedRecordList(IStore store, int element_size) {
			this.store = store;
			this.element_size = element_size;
			list_block_element = new long[64];
			list_block_area = new IMutableArea[64];
		}

		/// <summary>
		/// Creates the structure in the store, and returns a pointer to the structure.
		/// </summary>
		/// <returns>
		/// Returns the pointer to the structure within the underlying store.
		/// </returns>
		public long Create() {
			// Allocate space for the list header (8 + 8 + (64 * 8))
			IAreaWriter writer = store.CreateArea(528);
			list_header_p = writer.Id;
			writer.WriteInt4(MAGIC);
			writer.Finish();

			list_header_area = store.GetMutableArea(list_header_p);
			list_block_count = 0;
			UpdateListHeaderArea();

			return list_header_p;
		}

		/// <summary>
		/// Initializes the structure from the store.
		/// </summary>
		/// <param name="listPointer">The pointer to the <see cref="IArea"/> within
		/// the underlying store from which to recover the information to
		/// initialize the structure.</param>
		public void Init(long listPointer) {
			list_header_p = listPointer;
			list_header_area = store.GetMutableArea(list_header_p);

			int magic = list_header_area.ReadInt4();          // MAGIC
			if (magic != MAGIC)
				throw new IOException("Incorrect magic for list block. [magic=" +
				                      magic + "]");

			list_block_count = list_header_area.ReadInt4();
			list_header_area.ReadInt8();
			for (int i = 0; i < list_block_count; ++i) {
				long block_pointer = list_header_area.ReadInt8();
				list_block_element[i] = block_pointer;
				list_block_area[i] = store.GetMutableArea(block_pointer);
			}
		}

		/// <summary>
		/// Adds to the given list all the pointers (as <see cref="long"/>) to the areas in the 
		/// store that are used by this structure.
		/// </summary>
		/// <param name="list">The destination list where to copy the pointers.</param>
		public void AddAllAreasUsed(ArrayList list) {
			list.Add(list_header_p);
			for (int i = 0; i < list_block_count; ++i) {
				list.Add(list_block_element[i]);
			}
		}

		/// <summary>
		/// Returns the 8 byte long that is reserved for storing the delete 
		/// chain (if there is one).
		/// </summary>
		/// <returns>
		/// </returns>
		public long ReadReservedLong() {
			list_header_area.Position = 8;
			return list_header_area.ReadInt8();
		}

		/// <summary>
		/// Sets the 8 byte long that is reserved for storing the delete chain
		/// (if there is one).
		/// </summary>
		/// <param name="v"></param>
		public void WriteReservedLong(long v) {
			list_header_area.Position = 8;
			list_header_area.WriteInt8(v);
			list_header_area.CheckOut();
		}

		/// <summary>
		/// Updates the list header area from the information store within 
		/// the state of this object.
		/// </summary>
		/// <remarks>
		/// This should only be called when a new block is added to the list 
		/// block, or the store is created.
		/// </remarks>
		private void UpdateListHeaderArea() {
			list_header_area.Position = 4;
			list_header_area.WriteInt4(list_block_count);
			list_header_area.Position = 16;
			for (int i = 0; i < list_block_count; ++i) {
				list_header_area.WriteInt8(list_block_element[i]);
			}
			list_header_area.CheckOut();
		}

		/// <summary>
		/// Returns an <see cref="IArea"/> object from the list block area with 
		/// the position over the record entry requested.
		/// </summary>
		/// <param name="recordNumber"></param>
		/// <remarks>
		/// The <see cref="IArea"/> object can only be safely used if there is a guarentee 
		/// that no other access to this object while the area object is accessed.
		/// </remarks>
		/// <returns></returns>
		public IMutableArea PositionOnNode(long recordNumber) {
			// What block is this record in?
			int bit = 0;
			long work = recordNumber + 32;
			while (work != 0) {
				work = work >> 1;
				++bit;
			}
			long start_val = (1 << (bit - 1)) - 32;
			int block_offset = bit - 6;
			long record_offset = recordNumber - start_val;

			// Get the pointer to the block that contains this record status
			IMutableArea block_area = list_block_area[block_offset];
			//    long tempv = (record_offset * element_size);
			//    int position_to = (int) tempv;
			//    if (tempv == 1) {
			//      ++tempv;
			//    }
			//    block_area.position(position_to);
			block_area.Position = (int)(record_offset * element_size);

			return block_area;
		}

		/// <summary>
		/// Returns the number of block elements in this list structure.
		/// </summary>
		/// <value>
		/// This will return a number between 0 and 63 (inclusive).
		/// </value>
		public int ListBlockCount {
			get { return list_block_count; }
		}

		/// <summary>
		/// Returns the total number of nodes that are currently addressable by 
		/// this list structure.
		/// </summary>
		public long AddressableNodeCount {
			get {
				/**
 * For example, if the list contains 0 blocks then there are
 * no addressable nodes.  If it contains 1 block, there are 32 addressable
 * nodes.  If it contains 2 blocks, there are 64 + 32 = 96 nodes.  3 blocks =
 * 128 + 64 + 32 = 224 nodes.
 */
				return ListBlockFirstPosition(list_block_count);
			}
		}


		/// <summary>
		/// Returns the number of nodes that can be stored in the given block, where
		/// block 0 is the first block (32 addressable nodes).
		/// </summary>
		/// <param name="block_number"></param>
		/// <returns></returns>
		public long ListBlockNodeCount(int block_number) {
			return 32L << block_number;
		}

		/// <summary>
		/// Returns the index of the first node in the given block number.
		/// </summary>
		public long ListBlockFirstPosition(int block_number) {
			/**
 * For example, this first node of block 0 is 0, the first node of block 1 is
 * 32, the first node of block 2 is 96, etc.
 */
			long start_index = 0;
			int i = block_number;
			long diff = 32;
			while (i > 0) {
				start_index = start_index + diff;
				diff = diff << 1;
				--i;
			}
			return start_index;
		}

		/// <summary>
		/// Increases the size of the list structure so it may accomodate more
		/// record entries.
		/// </summary>
		/// <remarks>
		/// This simply adds a new block for more nodes.
		/// </remarks>
		public void IncreaseSize() {
			// The size of the block
			long size_of_block = 32L << list_block_count;
			// Allocate the new block in the store
			IAreaWriter writer = store.CreateArea(size_of_block * element_size);
			long nblock_p = writer.Id;
			writer.Finish();
			IMutableArea nblock_area = store.GetMutableArea(nblock_p);
			// Update the block list
			list_block_element[list_block_count] = nblock_p;
			list_block_area[list_block_count] = nblock_area;
			++list_block_count;
			// Update the list header,
			UpdateListHeaderArea();
		}

		/// <summary>
		/// Decreases the size of the list structure.
		/// </summary>
		/// <remarks>
		/// This should be used with care because it deletes all nodes in the 
		/// last block.
		/// </remarks>
		public void DecreaseSize() {
			--list_block_count;
			// Free the top block
			store.DeleteArea(list_block_element[list_block_count]);
			// Help the GC
			list_block_area[list_block_count] = null;
			list_block_element[list_block_count] = 0;
			// Update the list header.
			UpdateListHeaderArea();
		}
	}
}