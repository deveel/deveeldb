//  
//  AbstractStore.cs
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

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	/// <summary>
	/// Provides an abstract implementation of <see cref="Store"/>.
	/// </summary>
	/// <remarks>
	/// This implements a bin based best-fit recycling algorithm. The 
	/// store manages a structure that points to bins of freed space 
	/// of specific sizes.  When an allocation is requested the structure 
	/// is searched for the first bin that contains an area that best fits
	/// the size requested.
	/// <para>
	/// Provided the derived class supports safe atomic I/O operations, this store
	/// is designed for robustness to the level that at no point is the store left
	/// input a unworkable (corrupt) state.
	/// </para>
	/// </remarks>
	public abstract class AbstractStore : IStore {
		/// <summary>
		/// The free bin list contains 128 entries pointing to the first available
		/// block input the bin.
		/// </summary>
		/// <remarks>
		/// If the list item contains -1 then there are no free blocks input the bin.
		/// </remarks>
		protected long[] free_bin_list;

		/// <summary>
		/// A pointer to the wilderness area (the last deleted area input the store),
		/// or -1 if there is no wilderness area.
		/// </summary>
		protected long wilderness_pointer;

		/// <summary>
		/// True if this is Read-only.
		/// </summary>
		protected bool read_only;

		/// <summary>
		/// The total amount of allocated space within this store since the store was 
		/// openned.
		/// </summary>
		/// <remarks>
		/// Note that this could be a negative amount if more space was freed than 
		/// allocated.
		/// </remarks>
		protected long total_allocated_space;

		/// <summary>
		/// True if the store was opened dirtily (was not previously closed cleanly).
		/// </summary>
		private bool dirty_open;

		// ---------- Statics ----------

		/// <summary>
		/// The offset into the file that the data areas start.
		/// </summary>
		protected const long DataAreaOffset = 256 + 1024 + 32;

		/// <summary>
		/// The offset into the file of the 64 byte fixed area.
		/// </summary>
		protected const long FixedAreaOffset = 128;

		/// <summary>
		/// The offset into the file that the bin area starts.
		/// </summary>
		protected const long BinAreaOffset = 256;

		/// <summary>
		/// The magic value.
		/// </summary>
		protected const int Magic = 0x0AEAE91;

		private const long ActiveFlag = Int64.MaxValue;
		private const long DeletedFlag = Int64.MinValue;

		/// <summary>
		/// Constructs the store.
		/// </summary>
		/// <param name="read_only"></param>
		protected AbstractStore(bool read_only) {
			free_bin_list = new long[BIN_ENTRIES + 1];
			for (int i = 0; i < BIN_ENTRIES + 1; ++i) {
				free_bin_list[i] = -1L;
			}
			wilderness_pointer = -1;
			this.read_only = read_only;
		}

		/// <summary>
		/// Initializes the store to an empty state.
		/// </summary>
		private void InitializeToEmpty() {
			lock (this) {
				SetDataAreaSize(DataAreaOffset);
				// New file so Write output the initial file area,
				MemoryStream bout = new MemoryStream((int)BinAreaOffset);
				BinaryWriter output = new BinaryWriter(bout, Encoding.Unicode);
				// The file MAGIC
				output.Write(Magic); // 0
				// The file version
				output.Write(1); // 4
				// The number of areas (chunks) input the file (currently unused)
				output.Write(-1L); // 8
				// File open/close status byte
				output.Write((byte)0); // 16

				output.Flush();
				byte[] buf = new byte[(int)DataAreaOffset];
				byte[] buf2 = bout.ToArray();
				Array.Copy(buf2, 0, buf, 0, buf2.Length);
				;
				for (int i = (int)BinAreaOffset; i < (int)DataAreaOffset; ++i) {
					buf[i] = (byte)255;
				}

				WriteByteArrayTo(0, buf, 0, buf.Length);
			}
		}


		/// <summary>
		/// Opens the data store.
		/// </summary>
		/// <returns>
		/// Returns <b>true</b> if the store did not close cleanly.
		/// </returns>
		public bool Open() {
			lock (this) {
				InternalOpen(read_only);

				// If it's small, initialize to empty
				if (EndOfDataAreaPointer < DataAreaOffset) {
					InitializeToEmpty();
				}

				byte[] read_buf = new byte[(int)BinAreaOffset];
				ReadByteArrayFrom(0, read_buf, 0, read_buf.Length);
				MemoryStream b_in = new MemoryStream(read_buf);
				BinaryReader din = new BinaryReader(b_in, Encoding.Unicode);

				int magic = din.ReadInt32();
				if (magic != Magic)
					throw new IOException("Format invalid: Magic value is not as expected.");

				int version = din.ReadInt32();
				if (version != 1)
					throw new IOException("Format invalid: unrecognised version.");

				din.ReadInt64(); // ignore
				byte status = din.ReadByte();
				dirty_open = false;
				if (status == 1) {
					// This means the store wasn't closed cleanly.
					dirty_open = true;
				}

				// Read the bins
				ReadBins();

				// Mark the file as open
				if (!read_only) {
					WriteByteTo(16, 1);
				}

				long file_length = EndOfDataAreaPointer;
				if (file_length <= 8) {
					throw new IOException("Format invalid: File size is too small.");
				}

				// Set the wilderness pointer.
				if (file_length == DataAreaOffset) {
					wilderness_pointer = -1;
				} else {
					ReadByteArrayFrom(file_length - 8, read_buf, 0, 8);
					long last_boundary = ByteBuffer.ReadInt8(read_buf, 0);
					long last_area_pointer = file_length - last_boundary;

					if (last_area_pointer < DataAreaOffset) {
						Console.Out.WriteLine("last_boundary = " + last_boundary);
						Console.Out.WriteLine("last_area_pointer = " + last_area_pointer);
						throw new IOException("File corrupt: last_area_pointer is before data part of file.");
					}
					if (last_area_pointer > file_length - 8) {
						throw new IOException("File corrupt: last_area_pointer at the end of the file.");
					}

					ReadByteArrayFrom(last_area_pointer, read_buf, 0, 8);
					long last_area_header = ByteBuffer.ReadInt8(read_buf, 0);
					// If this is a freed block, then set this are the wilderness pointer.
					if ((last_area_header & DeletedFlag /* 0x08000000000000000L */) != 0) {
						wilderness_pointer = last_area_pointer;
					} else {
						wilderness_pointer = -1;
					}
				}

				return dirty_open;
			}
		}

		/// <summary>
		/// Closes the store.
		/// </summary>
		public void Close() {
			lock (this) {
				// Mark the file as closed
				if (!read_only) {
					WriteByteTo(16, 0);
				}

				InternalClose();
			}
		}

		/// <summary>
		/// Checks if the given area size is valid.
		/// </summary>
		/// <param name="size"></param>
		/// <remarks>
		/// The criteria for a valid boundary size is (<paramref name="size"/> &gt;= 24) and 
		/// (<paramref name="size"/> % 8 == 0) and (<paramref name="size"/> &lt; 200 gigabytes)
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the given <param name="size"/> is valid.
		/// </returns>
		protected static bool IsValidBoundarySize(long size) {
			const long MAX_AREA_SIZE = (long)Int32.MaxValue * 200;
			size = size & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
			return ((size < MAX_AREA_SIZE) && (size >= 24) && ((size & 0x07) == 0));
		}

		private readonly byte[] buf = new byte[8];
		/// <summary>
		/// Reads an 8 byte long at the given position input the data area.
		/// </summary>
		private long readLongAt(long position) {
			ReadByteArrayFrom(position, buf, 0, 8);
			return ByteBuffer.ReadInt8(buf, 0);
		}

		/// <summary>
		/// Performs a repair scan from the given pointer.
		/// </summary>
		/// <param name="areas_to_fix"></param>
		/// <param name="pointer"></param>
		/// <param name="end_pointer"></param>
		/// <param name="scan_forward"></param>
		/// <param name="max_repairs"></param>
		/// <remarks>
		/// This is a recursive algorithm that looks for at most 'n' number 
		/// of repairs before giving up.
		/// </remarks>
		/// <returns>
		/// Returns false if a repair path could not be found.
		/// </returns>
		private bool RepairScan(ArrayList areas_to_fix, long pointer, long end_pointer, bool scan_forward, int max_repairs) {
			// Recurse end conditions;
			// If the end is reached, success!
			if (pointer == end_pointer) {
				return true;
			}
			// If max repairs exhausted, failure!
			if (pointer > end_pointer || max_repairs <= 0) {
				return false;
			}

			long pointer_to_head = scan_forward ? pointer : end_pointer - 8;

			// Does the pointer at least look right?
			long first_header = readLongAt(pointer_to_head) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
			// If it's a valid boundary size, and the header points inside the
			// end boundary
			long max_bound_size = end_pointer - pointer;
			if (IsValidBoundarySize(first_header) && first_header <= max_bound_size) {

				long pointer_to_tail = scan_forward ? (pointer + first_header) - 8 :
													  end_pointer - first_header;

				// If the end doesn't look okay,
				long end_area_pointer = pointer_to_tail;
				long end_header = readLongAt(end_area_pointer) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
				bool valid_end_header = (first_header == end_header);

				long scan_area_p1 = scan_forward ? (pointer + first_header) : pointer;
				long scan_area_p2 = scan_forward ? end_pointer : (end_pointer - first_header);

				if (!valid_end_header) {
					// First and ends are invalid, so lets first assume we make the end
					// valid and recurse,
					long area_p = scan_forward ? pointer_to_head : pointer_to_tail;
					areas_to_fix.Add(area_p);
					areas_to_fix.Add(first_header);

					bool b = RepairScan(areas_to_fix, scan_area_p1, scan_area_p2,
										   true, max_repairs - 1);
					// If success
					if (b) {
						return true;
					}

					// If failure, take that repair off the top
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
					// And keep searching
				} else {
					// Looks okay, so keep going,

					// This really does the same thing as recursing through the scan area
					// however, we have to reduce the stack usage for large files which
					// makes this iterative solution necessary.  Basically, this looks for
					// the first broken area and reverts back to applying the recursive
					// algorithm on it.
					bool something_broken = false;
					long previous1_scan_area_p1 = scan_area_p1;
					long previous2_scan_area_p1 = scan_area_p1;
					long previous3_scan_area_p1 = scan_area_p1;

					while (scan_area_p1 < scan_area_p2 && !something_broken) {
						// Assume something broken,
						something_broken = true;

						// Does the pointer at least look right?
						long scanning_header =
											readLongAt(scan_area_p1) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
						long scan_max_bound_size = scan_area_p2 - scan_area_p1;
						if (IsValidBoundarySize(scanning_header) &&
							scanning_header <= scan_max_bound_size) {
							long scan_end_header =
											readLongAt((scan_area_p1 + scanning_header) - 8)
											& ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
							if (scan_end_header == scanning_header) {
								// Cycle the scanned areas
								previous3_scan_area_p1 = previous2_scan_area_p1;
								previous2_scan_area_p1 = previous1_scan_area_p1;
								previous1_scan_area_p1 = scan_area_p1;

								scan_area_p1 = (scan_area_p1 + scanning_header);
								// Enough evidence that area is not broken, so continue scan
								something_broken = false;
							}
						}
					}
					if (something_broken) {
						// Back track to the last 3 scanned areas and perform a repair on
						// this area.  This allows for the scan to have more choices on
						// repair paths.
						scan_area_p1 = previous3_scan_area_p1;
					}

					// The recursive scan on the (potentially) broken area.
					bool b = RepairScan(areas_to_fix, scan_area_p1, scan_area_p2,
										   true, max_repairs);
					if (b) {
						// Repair succeeded!
						return b;
					}

					// Repair didn't succeed so keep searching.
				}
			}

			// Try reversing the scan and see if that comes up with something valid.
			if (scan_forward) {
				bool b = RepairScan(areas_to_fix, pointer, end_pointer,
									   false, max_repairs);
				// Success
				if (b) {
					return b;
				}

			} else {
				return false;
			}

			// We guarenteed to be scan forward if we get here....

			// Facts: we know that the start and end pointers are invalid....

			// Search forward for something that looks like a boundary.  If we don't
			// find it, search backwards for something that looks like a boundary.

			long max_size = end_pointer - pointer;
			for (long i = 16; i < max_size; i += 8) {

				long v = readLongAt(pointer + i) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
				if (v == i + 8) {
					// This looks like a boundary, so try this...
					areas_to_fix.Add(pointer);
					areas_to_fix.Add(i + 8);
					bool b = RepairScan(areas_to_fix, pointer + i + 8, end_pointer,
										   true, max_repairs - 1);
					if (b) {
						return true;
					}
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
				}

			}

			// Scan backwards....
			for (long i = max_size - 8 - 16; i >= 0; i -= 8) {

				long v = readLongAt(pointer + i) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
				if (v == (max_size - i)) {
					// This looks like a boundary, so try this...
					areas_to_fix.Add(pointer + i);
					areas_to_fix.Add((max_size - i));
					bool b = RepairScan(areas_to_fix, pointer, pointer + i,
										   true, max_repairs - 1);
					if (b) {
						return true;
					}
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
					areas_to_fix.RemoveAt(areas_to_fix.Count - 1);
				}

			}

			// No luck, so simply set this as a final big area and return true.
			// NOTE: There are other tests possible here but I think what we have will
			//   find fixes for 99% of corruption cases.
			areas_to_fix.Add(pointer);
			areas_to_fix.Add(end_pointer - pointer);

			return true;

		}

		///<summary>
		/// Opens/scans the store looking for any errors with the layout and
		/// if a problem with the store is detected, it attempts to fix it.
		///</summary>
		///<param name="terminal"></param>
		public void OpenScanAndFix(IUserTerminal terminal) {
			lock (this) {
				InternalOpen(read_only);

				terminal.WriteLine("- Store: " + ToString());

				// If it's small, initialize to empty
				if (EndOfDataAreaPointer < DataAreaOffset) {
					terminal.WriteLine("+ Store too small - initializing to empty.");
					InitializeToEmpty();
					return;
				}

				byte[] read_buf = new byte[(int)BinAreaOffset];
				ReadByteArrayFrom(0, read_buf, 0, read_buf.Length);
				MemoryStream b_in = new MemoryStream(read_buf);
				BinaryReader din = new BinaryReader(b_in, Encoding.Unicode);

				int magic = din.ReadInt32();
				if (magic != Magic) {
					terminal.WriteLine("! Store magic value not present - not fixable.");
					return;
				}
				int version = din.ReadInt32();
				if (version != 1) {
					terminal.WriteLine("! Store version is invalid - not fixable.");
					return;
				}

				// Check the size
				long end_of_data_area = EndOfDataAreaPointer;
				if (end_of_data_area < DataAreaOffset + 16) {
					// Store size is too small.  There's nothing to be lost be simply
					// reinitializing it to a blank state.
					terminal.WriteLine("! Store is too small, reinitializing store to blank state.");
					InitializeToEmpty();
					return;
				}

				// Do a recursive scan over the store.
				ArrayList repairs = new ArrayList();
				bool b = RepairScan(repairs, DataAreaOffset, EndOfDataAreaPointer, true, 20);

				if (b) {
					if (repairs.Count == 0) {
						terminal.WriteLine("- Store areas are intact.");
					} else {
						terminal.WriteLine("+ " + (repairs.Count / 2) + " area repairs:");
						for (int i = 0; i < repairs.Count; i += 2) {
							terminal.WriteLine("  IArea pointer: " + repairs[i]);
							terminal.WriteLine("  IArea size: " + repairs[i + 1]);
							long pointer = (long)repairs[i];
							long size = (long)repairs[i + 1];
							CoalescArea(pointer, size);
						}
					}
				} else {
					terminal.WriteLine("- Store is not repairable!");
				}

				// Rebuild the free bins,
				free_bin_list = new long[BIN_ENTRIES + 1];
				for (int i = 0; i < BIN_ENTRIES + 1; ++i) {
					free_bin_list[i] = (long)-1;
				}

				terminal.WriteLine("+ Rebuilding free bins.");
				long[] header = new long[2];
				// Scan for all free areas input the store.
				long areaPointer = DataAreaOffset;
				while (areaPointer < end_of_data_area) {
					GetAreaHeader(areaPointer, header);
					long area_size = (header[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
					bool is_free = ((header[0] & DeletedFlag /* 0x08000000000000000L */) != 0);

					if (is_free) {
						AddToBinChain(areaPointer, area_size);
					}

					areaPointer += area_size;
				}

				// Update all the bins
				WriteAllBins();

				terminal.WriteLine("- Store repair complete.");

				// Open the store for real,
				Open();

			}
		}

		///<summary>
		/// Performs an extensive lookup on all the tables input this store and sets a number 
		/// of properties input the given <see cref="Hashtable"/> (property name(String) -> 
		/// property description(Object)).
		///</summary>
		///<param name="properties"></param>
		/// <remarks>
		/// This should be used for store diagnostics.
		/// <para>
		/// Assume the store is open.
		/// </para>
		/// </remarks>
		public void StatsScan(Hashtable properties) {
			lock (this) {
				long free_areas = 0;
				long free_total = 0;
				long allocated_areas = 0;
				long allocated_total = 0;

				long end_of_data_area = EndOfDataAreaPointer;

				long[] header = new long[2];
				// The first header
				long pointer = DataAreaOffset;
				while (pointer < end_of_data_area) {
					GetAreaHeader(pointer, header);
					long area_size = (header[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);

					if ((header[0] & DeletedFlag /* 0x08000000000000000L */) != 0) {
						++free_areas;
						free_total += area_size;
					} else {
						++allocated_areas;
						allocated_total += area_size;
					}

					pointer += area_size;
				}

				if (wilderness_pointer != -1) {
					GetAreaHeader(wilderness_pointer, header);
					long wilderness_size = (header[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
					properties["AbstractStore.wilderness_size"] = wilderness_size;
				}

				properties["AbstractStore.end_of_data_area"] = end_of_data_area;
				properties["AbstractStore.free_areas"] = free_areas;
				properties["AbstractStore.free_total"] = free_total;
				properties["AbstractStore.allocated_areas"] = allocated_areas;
				properties["AbstractStore.allocated_total"] = allocated_total;

			}
		}

		/// <summary>
		/// Returns a <see cref="IList"/> of <see cref="long"/> objects that contain 
		/// a complete list of all areas input the store.
		/// </summary>
		/// <remarks>
		/// This is useful for checking if a given pointer is valid or not. The returned 
		/// list is sorted from start area to end area.
		/// </remarks>
		/// <returns></returns>
		public IList GetAllAreas() {
			ArrayList list = new ArrayList();
			long end_of_data_area = EndOfDataAreaPointer;
			long[] header = new long[2];
			// The first header
			long pointer = DataAreaOffset;
			while (pointer < end_of_data_area) {
				GetAreaHeader(pointer, header);
				long area_size = (header[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
				if ((header[0] & DeletedFlag /* 0x08000000000000000L */) == 0) {
					list.Add(pointer);
				}
				pointer += area_size;
			}
			return list;
		}

		///<summary>
		/// Scans the area list, and any areas that aren't deleted and aren't found 
		/// input the given <see cref="ArrayList"/> are returned as leaked areas. 
		///</summary>
		///<param name="list"></param>
		/// <remarks>
		/// This is a useful method for finding any leaks input the store.
		/// </remarks>
		///<returns></returns>
		///<exception cref="IOException"></exception>
		public ArrayList FindAllocatedAreasNotIn(ArrayList list) {

			// Sort the list
			list.Sort();

			// The list of leaked areas
			ArrayList leaked_areas = new ArrayList();

			int list_index = 0;

			// What area are we looking for?
			long looking_for = Int64.MaxValue;
			if (list_index < list.Count) {
				looking_for = (long)list[list_index];
				++list_index;
			}

			long end_of_data_area = EndOfDataAreaPointer;
			long[] header = new long[2];

			long pointer = DataAreaOffset;
			while (pointer < end_of_data_area) {
				GetAreaHeader(pointer, header);
				long area_size = (header[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
				bool area_free = (header[0] & DeletedFlag /* 0x08000000000000000L */) != 0;

				if (pointer == looking_for) {
					if (area_free) {
						throw new IOException("IArea (pointer = " + pointer + ") is not allocated!");
					}
					// Update the 'looking_for' pointer
					if (list_index < list.Count) {
						looking_for = (long)list[list_index];
						++list_index;
					} else {
						looking_for = Int64.MaxValue;
					}
				} else if (pointer > looking_for) {
					throw new IOException("IArea (pointer = " + looking_for + ") wasn't found input store!");
				} else {
					// An area that isn't input the list
					if (!area_free) {
						// This is a leaked area.
						// It isn't free and it isn't input the list
						leaked_areas.Add(pointer);
					}
				}

				pointer += area_size;
			}

			return leaked_areas;
		}


		///<summary>
		/// Returns the total allocated space since the file was openned.
		///</summary>
		public long TotalAllocatedSinceStart {
			get {
				lock (this) {
					return total_allocated_space;
				}
			}
		}

		/// <summary>
		/// Returns the bin index that would be the minimum size to store the given object.
		/// </summary>
		/// <param name="size"></param>
		/// <returns></returns>
		private static int MinimumBinSizeIndex(long size) {
			int i = Array.BinarySearch(BIN_SIZES, (int)size);
			if (i < 0) {
				i = -(i + 1);
			}
			return i;
		}

		/// <summary>
		/// Internally opens the backing area.
		/// </summary>
		/// <param name="read_only">If is true then the store is openned input 
		/// read only mode.</param>
		protected abstract void InternalOpen(bool read_only);

		/// <summary>
		/// Internally closes the backing area.
		/// </summary>
		protected abstract void InternalClose();

		/// <summary>
		/// Reads a byte from the given position input the file.
		/// </summary>
		/// <param name="position"></param>
		/// <returns></returns>
		protected abstract int ReadByteFrom(long position);

		/// <summary>
		/// Reads a byte array from the given position input the file.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		/// <returns>
		/// Returns the number of bytes read.
		/// </returns>
		protected abstract int ReadByteArrayFrom(long position, byte[] buf, int off, int len);

		/// <summary>
		/// Writes a byte to the given position input the file.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="b"></param>
		protected abstract void WriteByteTo(long position, int b);

		/// <summary>
		/// Writes a byte array to the given position input the file.
		/// </summary>
		/// <param name="position"></param>
		/// <param name="buf"></param>
		/// <param name="off"></param>
		/// <param name="len"></param>
		protected abstract void WriteByteArrayTo(long position, byte[] buf, int off, int len);

		/// <summary>
		/// Returns a pointer to the end of the current data area.
		/// </summary>
		protected abstract long EndOfDataAreaPointer { get; }

		/// <summary>
		/// Sets the size of the data area.
		/// </summary>
		/// <param name="length"></param>
		protected abstract void SetDataAreaSize(long length);


		// ----------

		/// <summary>
		/// Checks the pointer is valid.
		/// </summary>
		/// <param name="pointer"></param>
		protected void CheckPointer(long pointer) {
			if (pointer < DataAreaOffset || pointer >= EndOfDataAreaPointer) {
				throw new IOException("Pointer output of range: " + DataAreaOffset +
									  " > " + pointer + " > " + EndOfDataAreaPointer);
			}
		}

		/// <summary>
		/// A buffered work area we work with when reading/writing bin pointers from
		/// the file header.
		/// </summary>
		private readonly byte[] bin_area = new byte[128 * 8];

		/// <summary>
		/// Reads the bins from the header information input the file.
		/// </summary>
		protected void ReadBins() {
			ReadByteArrayFrom(BinAreaOffset, bin_area, 0, 128 * 8);
			MemoryStream bin = new MemoryStream(bin_area);
			BinaryReader input = new BinaryReader(bin, Encoding.Unicode);
			for (int i = 0; i < 128; ++i) {
				free_bin_list[i] = input.ReadInt64();
			}
		}

		/// <summary>
		/// Updates all bins to the data area header area.
		/// </summary>
		protected void WriteAllBins() {
			int p = 0;
			for (int i = 0; i < 128; ++i, p += 8) {
				long val = free_bin_list[i];
				ByteBuffer.WriteInt8(val, bin_area, p);
			}
			WriteByteArrayTo(BinAreaOffset, bin_area, 0, 128 * 8);
		}

		/// <summary>
		/// Updates the given bin index to the data area header area.
		/// </summary>
		/// <param name="index"></param>
		protected void WriteBinIndex(int index) {
			int p = index * 8;
			long val = free_bin_list[index];
			ByteBuffer.WriteInt8(val, bin_area, p);
			WriteByteArrayTo(BinAreaOffset + p, bin_area, p, 8);
		}

		protected readonly byte[] header_buf = new byte[16];

		/// <summary>
		/// Sets the <paramref name="header"/> array with information from the header 
		/// of the given pointer.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="header"></param>
		protected void GetAreaHeader(long pointer, long[] header) {
			ReadByteArrayFrom(pointer, header_buf, 0, 16);
			header[0] = ByteBuffer.ReadInt8(header_buf, 0);
			header[1] = ByteBuffer.ReadInt8(header_buf, 8);
		}

		/// <summary>
		/// Sets the <paramref name="header"/> array with information from the previous header 
		/// to the given pointer.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="header"></param>
		/// <returns>
		/// Returns a pointer to the previous area.
		/// </returns>
		protected long GetPreviousAreaHeader(long pointer, long[] header) {
			// If the pointer is the start of the file area
			if (pointer == DataAreaOffset) {
				// Return a 0 sized block
				header[0] = 0;
				return -1;
			}

			ReadByteArrayFrom(pointer - 8, header_buf, 0, 8);
			long sz = ByteBuffer.ReadInt8(header_buf, 0);
			sz = sz & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
			long previous_pointer = pointer - sz;
			ReadByteArrayFrom(previous_pointer, header_buf, 0, 8);
			header[0] = ByteBuffer.ReadInt8(header_buf, 0);
			return previous_pointer;
		}

		/// <summary>
		/// Sets the <paramref name="header"/> array with information from the next header 
		/// to the given pointer.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="header"></param>
		/// <returns>
		/// Returns a pointer to the next area.
		/// </returns>
		protected long GetNextAreaHeader(long pointer, long[] header) {
			ReadByteArrayFrom(pointer, header_buf, 0, 8);
			long sz = ByteBuffer.ReadInt8(header_buf, 0);
			sz = sz & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
			long next_pointer = pointer + sz;

			if (next_pointer >= EndOfDataAreaPointer) {
				// Return a 0 sized block
				header[0] = 0;
				return -1;
			}

			ReadByteArrayFrom(next_pointer, header_buf, 0, 8);
			header[0] = ByteBuffer.ReadInt8(header_buf, 0);
			return next_pointer;
		}

		/// <summary>
		/// Rebounds the given area with the given header information. 
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="header"></param>
		/// <param name="write_headers">If is true, the header (header[0]) is changed.</param>
		/// <remarks>
		/// Note that this shouldn't be used to change the size of a chunk.
		/// </remarks>
		protected void ReboundArea(long pointer, long[] header, bool write_headers) {
			if (write_headers) {
				ByteBuffer.WriteInt8(header[0], header_buf, 0);
				ByteBuffer.WriteInt8(header[1], header_buf, 8);
				WriteByteArrayTo(pointer, header_buf, 0, 16);
			} else {
				ByteBuffer.WriteInt8(header[1], header_buf, 8);
				WriteByteArrayTo(pointer + 8, header_buf, 8, 8);
			}
		}

		/// <summary>
		/// Coalesc one or more areas into a larger area.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// This alters the boundary of the area to encompass the given size.
		/// </remarks>
		protected void CoalescArea(long pointer, long size) {
			ByteBuffer.WriteInt8(size, header_buf, 0);

			// ISSUE: Boundary alteration is a moment when corruption could occur.
			//   There are two seeks and writes here and when we are setting the
			//   end points, there is a risk of failure.

			WriteByteArrayTo(pointer, header_buf, 0, 8);
			WriteByteArrayTo((pointer + size) - 8, header_buf, 0, 8);
		}

		/// <summary>
		/// Expands the data area by at least the minimum size given.
		/// </summary>
		/// <param name="minimum_size"></param>
		/// <returns>
		/// Returns the actual size the data area was expanded by.
		/// </returns>
		protected long ExpandDataArea(long minimum_size) {
			long end_of_data_area = EndOfDataAreaPointer;

			// Round all sizes up to the nearest 8
			// We grow only by a small amount if the area is small, and a large amount
			// if the area is large.
			long over_grow = end_of_data_area / 64;
			long d = (over_grow & 0x07L);
			if (d != 0) {
				over_grow = over_grow + (8 - d);
			}
			over_grow = System.Math.Min(over_grow, 262144L);
			if (over_grow < 1024) {
				over_grow = 1024;
			}

			long grow_by = minimum_size + over_grow;
			long new_file_length = end_of_data_area + grow_by;
			SetDataAreaSize(new_file_length);
			return grow_by;
		}

		/// <summary>
		/// Splits an area pointed to by <paramref name="pointer"/> at a new 
		/// boundary point.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="new_boundary"></param>
		protected void SplitArea(long pointer, long new_boundary) {
			// Split the area pointed to by the pointer.
			ReadByteArrayFrom(pointer, header_buf, 0, 8);
			long cur_size = ByteBuffer.ReadInt8(header_buf, 0) & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */;
			long left_size = new_boundary;
			long right_size = cur_size - new_boundary;

			if (right_size < 0) {
				throw new ApplicationException("right_size < 0");
			}

			ByteBuffer.WriteInt8(left_size, header_buf, 0);
			ByteBuffer.WriteInt8(right_size, header_buf, 8);

			// ISSUE: Boundary alteration is a moment when corruption could occur.
			//   There are three seeks and writes here and when we are setting the
			//   end points, there is a risk of failure.

			// First set the boundary
			WriteByteArrayTo((pointer + new_boundary) - 8, header_buf, 0, 16);
			// Now set the end points
			WriteByteArrayTo(pointer, header_buf, 0, 8);
			WriteByteArrayTo((pointer + cur_size) - 8, header_buf, 8, 8);
		}




		private long[] header_info = new long[2];
		private long[] header_info2 = new long[2];


		/// <summary>
		/// Adds the given area to the bin represented by the bin_chain_index.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="size"></param>
		private void AddToBinChain(long pointer, long size) {

			CheckPointer(pointer);

			// What bin would this area fit into?
			int bin_chain_index = MinimumBinSizeIndex(size);

			//    Console.Out.WriteLine("+ Adding to bin chain: " + pointer + " size: " + size);
			//    Console.Out.WriteLine("+ Adding to index: " + bin_chain_index);

			long cur_pointer = free_bin_list[bin_chain_index];
			if (cur_pointer == -1) {
				// If the bin chain has no elements,
				header_info[0] = (size | DeletedFlag /* 0x08000000000000000L */);
				header_info[1] = -1;
				ReboundArea(pointer, header_info, true);
				free_bin_list[bin_chain_index] = pointer;
				WriteBinIndex(bin_chain_index);
			} else {
				bool inserted = false;
				long last_pointer = -1;
				int searches = 0;
				while (cur_pointer != -1 && inserted == false) {
					// Get the current pointer
					GetAreaHeader(cur_pointer, header_info);

					long header = header_info[0];
					long next = header_info[1];
					// Assert - the header must have deleted flag
					if ((header & DeletedFlag /* 0x08000000000000000L */) == 0) {
						throw new ApplicationException("Assert failed - area not marked as deleted.  " +
										"pos = " + cur_pointer +
										" this = " + ToString());
					}
					long area_size = header ^ DeletedFlag /* 0x08000000000000000L */;
					if (area_size >= size || searches >= 12) {
						// Insert if the area size is >= than the size we are adding.
						// Set the previous header to point to this
						long previous = last_pointer;

						// Set up the deleted area
						header_info[0] = (size | DeletedFlag /* 0x08000000000000000L */);
						header_info[1] = cur_pointer;
						ReboundArea(pointer, header_info, true);

						if (last_pointer != -1) {
							// Set the previous input the chain to point to the deleted area
							GetAreaHeader(previous, header_info);
							header_info[1] = pointer;
							ReboundArea(previous, header_info, false);
						} else {
							// Otherwise set the head bin item
							free_bin_list[bin_chain_index] = pointer;
							WriteBinIndex(bin_chain_index);
						}

						inserted = true;
					}
					last_pointer = cur_pointer;
					cur_pointer = next;
					++searches;
				}

				// If we reach the end and we haven't inserted,
				if (!inserted) {
					// Set the new deleted area.
					header_info[0] = (size | DeletedFlag /* 0x08000000000000000L */);
					header_info[1] = -1;
					ReboundArea(pointer, header_info, true);

					// Set the previous entry to this
					GetAreaHeader(last_pointer, header_info);
					header_info[1] = pointer;
					ReboundArea(last_pointer, header_info, false);
				}
			}
		}

		/// <summary>
		/// Removes the given area from the bin chain.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="size"></param>
		/// <remarks>
		/// This requires a search of the bin chain for the given size.
		/// </remarks>
		private void RemoveFromBinChain(long pointer, long size) {
			// What bin index should we be looking input?
			int bin_chain_index = MinimumBinSizeIndex(size);

			//    Console.Out.WriteLine("- Removing from bin chain " + pointer + " size " + size);
			//    Console.Out.WriteLine("- Removing from index " + bin_chain_index);

			long previous_pointer = -1;
			long cur_pointer = free_bin_list[bin_chain_index];
			// Search this bin for the pointer
			// NOTE: This is an iterative search through the bin chain
			while (pointer != cur_pointer) {
				if (cur_pointer == -1) {
					throw new IOException("IArea not found input bin chain!  " +
										  "pos = " + pointer + " store = " + ToString());
				}
				// Move to the next input the chain
				GetAreaHeader(cur_pointer, header_info);
				previous_pointer = cur_pointer;
				cur_pointer = header_info[1];
			}

			// Found the pointer, so remove it,
			if (previous_pointer == -1) {
				GetAreaHeader(pointer, header_info);
				free_bin_list[bin_chain_index] = header_info[1];
				WriteBinIndex(bin_chain_index);
			} else {
				GetAreaHeader(previous_pointer, header_info2);
				GetAreaHeader(pointer, header_info);
				header_info2[1] = header_info[1];
				ReboundArea(previous_pointer, header_info2, false);
			}

		}

		/// <summary>
		/// Crops the area to the given size.
		/// </summary>
		/// <param name="pointer"></param>
		/// <param name="allocated_size"></param>
		/// <remarks>
		/// This is used after an area is pulled from a bin. This method decides 
		/// if it's worth reusing any space left over and the end of the area.
		/// </remarks>
		private void CropArea(long pointer, long allocated_size) {
			// Get the header info
			GetAreaHeader(pointer, header_info);
			long header = header_info[0];
			// Can we recycle the difference input area size?
			long free_area_size = header;
			// The difference between the size of the free area and the size
			// of the allocated area?
			long size_difference = free_area_size - allocated_size;
			// If the difference is greater than 512 bytes, add the excess space to
			// a free bin.
			bool is_wilderness = (pointer == wilderness_pointer);
			if ((is_wilderness && size_difference >= 32) || size_difference >= 512) {
				// Split the area into two areas.
				SplitArea(pointer, allocated_size);

				long left_over_pointer = pointer + allocated_size;
				// Add this area to the bin chain
				AddToBinChain(left_over_pointer, size_difference);

				// If pointer is the wilderness area, set this as the new wilderness
				if (is_wilderness ||
					(left_over_pointer + size_difference) >= EndOfDataAreaPointer) {
					wilderness_pointer = left_over_pointer;
				}

			} else {
				// If pointer is the wilderness area, set wilderness to -1
				if (is_wilderness) {
					wilderness_pointer = -1;
				}
			}
		}

		/// <summary>
		/// Allocates a block of memory from the backing area of the given size
		/// </summary>
		/// <param name="size"></param>
		/// <returns>
		/// Returns a pointer to the area allocated.
		/// </returns>
		private long Alloc(long size) {

			// Negative allocations are not allowed
			if (size < 0) {
				throw new IOException("Negative size allocation");
			}

			// Add 16 bytes for headers
			size = size + 16;
			// If size < 32, make size = 32
			if (size < 32) {
				size = 32;
			}

			// Round all sizes up to the nearest 8
			long d = size & 0x07L;
			if (d != 0) {
				size = size + (8 - d);
			}

			long real_alloc_size = size;

			// Search the free bin list for the first bin that matches the given size.
			int bin_chain_index;
			if (size > MAX_BIN_SIZE) {
				bin_chain_index = BIN_ENTRIES;
			} else {
				int i = MinimumBinSizeIndex(size);
				bin_chain_index = i;
			}

			// Search the bins until we find the first area that is the nearest fit to
			// the size requested.
			int found_bin_index = -1;
			long previous_pointer = -1;
			bool first = true;
			for (int i = bin_chain_index;
				 i < BIN_ENTRIES + 1 && found_bin_index == -1; ++i) {
				long cur_pointer = free_bin_list[i];
				if (cur_pointer != -1) {
					if (!first) {
						// Pick this..
						found_bin_index = i;
						previous_pointer = -1;
					}
						// Search this bin for the first that's big enough.
						// We only search the first 12 entries input the bin before giving up.
					else {
						long last_pointer = -1;
						int searches = 0;
						while (cur_pointer != -1 &&
							   found_bin_index == -1 &&
							   searches < 12) {
							GetAreaHeader(cur_pointer, header_info);
							long area_size = (header_info[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
							// Is this area is greater or equal than the required size
							// and is not the wilderness area, pick it.
							if (cur_pointer != wilderness_pointer && area_size >= size) {
								found_bin_index = i;
								previous_pointer = last_pointer;
							}
							// Go to next input chain.
							last_pointer = cur_pointer;
							cur_pointer = header_info[1];
							++searches;
						}
					}

				}
				first = false;
			}

			// If no area can be recycled,
			if (found_bin_index == -1) {

				// Allocate a new area of the given size.
				// If there is a wilderness, grow the wilderness area to the new size,
				long working_pointer;
				long size_to_grow;
				long current_area_size;
				if (wilderness_pointer != -1) {
					working_pointer = wilderness_pointer;
					GetAreaHeader(wilderness_pointer, header_info);
					long wilderness_size = (header_info[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
					// Remove this from the bins
					RemoveFromBinChain(working_pointer, wilderness_size);
					// For safety, we set wilderness_pointer to -1
					wilderness_pointer = -1;
					size_to_grow = size - wilderness_size;
					current_area_size = wilderness_size;
				} else {
					// wilderness_pointer == -1 so add to the end of the data area.
					working_pointer = EndOfDataAreaPointer;
					size_to_grow = size;
					current_area_size = 0;
				}

				long expanded_size = 0;
				if (size_to_grow > 0) {
					// Expand the data area to the new size.
					expanded_size = ExpandDataArea(size_to_grow);
				}
				// Coalesc the new area to the given size
				CoalescArea(working_pointer, current_area_size + expanded_size);
				// crop the area
				CropArea(working_pointer, size);

				// Add to the total allocated space
				total_allocated_space += real_alloc_size;

				return working_pointer;
			} else {

				// An area is taken from the bins,
				long free_area_pointer;
				// Remove this area from the bin chain and possibly add any excess space
				// left over to a new bin.
				if (previous_pointer == -1) {
					free_area_pointer = free_bin_list[found_bin_index];
					GetAreaHeader(free_area_pointer, header_info);
					free_bin_list[found_bin_index] = header_info[1];
					WriteBinIndex(found_bin_index);
				} else {
					GetAreaHeader(previous_pointer, header_info2);
					free_area_pointer = header_info2[1];
					GetAreaHeader(free_area_pointer, header_info);
					header_info2[1] = header_info[1];
					ReboundArea(previous_pointer, header_info2, false);
				}

				// Reset the header of the recycled area.
				header_info[0] = (header_info[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);
				ReboundArea(free_area_pointer, header_info, true);

				// Crop the area to the given size.
				CropArea(free_area_pointer, size);

				// Add to the total allocated space
				total_allocated_space += real_alloc_size;

				return free_area_pointer;
			}
		}

		/// <summary>
		/// Frees a previously allocated area input the store.
		/// </summary>
		/// <param name="pointer"></param>
		private void Free(long pointer) {

			// Get the area header
			GetAreaHeader(pointer, header_info);

			if ((header_info[0] & DeletedFlag /* 0x08000000000000000L */) != 0) {
				throw new IOException("IArea already marked as unallocated.");
			}

			// If (pointer + size) reaches the end of the header area, set this as the
			// wilderness.
			bool set_as_wilderness =
								((pointer + header_info[0]) >= EndOfDataAreaPointer);

			long r_pointer = pointer;
			long freeing_area_size = header_info[0];
			long r_size = freeing_area_size;

			// Can this area coalesc?
			long left_pointer = GetPreviousAreaHeader(pointer, header_info2);
			bool coalesc = false;
			if ((header_info2[0] & DeletedFlag /* 0x08000000000000000L */) != 0) {
				// Yes, we can coalesc left
				long area_size = (header_info2[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);

				r_pointer = left_pointer;
				r_size = r_size + area_size;
				// Remove left area from the bin
				RemoveFromBinChain(left_pointer, area_size);
				coalesc = true;

			}

			if (!set_as_wilderness) {
				long right_pointer = GetNextAreaHeader(pointer, header_info2);
				if ((header_info2[0] & DeletedFlag /* 0x08000000000000000L */) != 0) {
					// Yes, we can coalesc right
					long area_size = (header_info2[0] & ActiveFlag /* 0x07FFFFFFFFFFFFFFFL */);

					r_size = r_size + area_size;
					// Remove right from the bin
					RemoveFromBinChain(right_pointer, area_size);
					set_as_wilderness = (right_pointer == wilderness_pointer);
					coalesc = true;

				}
			}

			// If we are coalescing parent areas
			if (coalesc) {
				CoalescArea(r_pointer, r_size);
			}

			// Add this new area to the bin chain,
			AddToBinChain(r_pointer, r_size);

			// Do we set this as the wilderness?
			if (set_as_wilderness) {
				wilderness_pointer = r_pointer;
			}

			total_allocated_space -= freeing_area_size;

		}

		/// <summary>
		/// Convenience for finding the size of an area.
		/// </summary>
		/// <param name="pointer"></param>
		/// <returns></returns>
		/// <exception cref="IOException">
		/// If the area identified by the given <paramref name="pointer"/> is deleted.
		/// </exception>
		private long GetAreaSize(long pointer) {
			byte[] buf = new byte[8];
			ReadByteArrayFrom(pointer, buf, 0, 8);
			long v = ByteBuffer.ReadInt8(buf, 0);
			if ((v & DeletedFlag /* 0x08000000000000000L */) != 0) {
				throw new IOException("IArea is deleted.");
			}
			return v - 16;
		}


		// ---------- Implemented from Store ----------

		public IAreaWriter CreateArea(long size) {
			lock (this) {
				long pointer = Alloc(size);
				return new StoreAreaWriter(this, pointer, size);
			}
		}

		public void DeleteArea(long id) {
			lock (this) {
				Free(id);
			}
		}

		public Stream GetAreaInputStream(long id) {
			if (id == -1) {
				return new StoreAreaInputStream(this, FixedAreaOffset, 64);
			} else {
				return new StoreAreaInputStream(this, id + 8, GetAreaSize(id));
			}
		}

		public IArea GetArea(long id) {
			// If this is the fixed area
			if (id == -1) {
				return new StoreArea(this, id, FixedAreaOffset, 64);
			}
				// Otherwise must be a regular area
			else {
				return new StoreArea(this, id, id);
			}
		}

		public IMutableArea GetMutableArea(long id) {
			// If this is the fixed area
			if (id == -1) {
				return new StoreMutableArea(this, id, FixedAreaOffset, 64);
			}
				// Otherwise must be a regular area
			else {
				return new StoreMutableArea(this, id, id);
			}
		}

		public abstract void LockForWrite();

		public abstract void UnlockForWrite();

		public abstract void CheckPoint();

		public bool LastCloseClean() {
			return !dirty_open;
		}

		// ---------- Inner classes ----------

		private class StoreAreaInputStream : Stream {
			private readonly AbstractStore store;
			private long pointer;
			private readonly long end_pointer;
			private long start_pointer;

			public StoreAreaInputStream(AbstractStore store, long pointer, long max_size) {
				this.store = store;
				this.pointer = start_pointer = pointer;
				end_pointer = pointer + max_size;
				// mark_point = -1;
			}

			public override int ReadByte() {
				if (pointer >= end_pointer) {
					return 0;
				}
				int b = store.ReadByteFrom(pointer);
				++pointer;
				return b;
			}

			public override bool CanSeek {
				get { return true; }
			}

			public override bool CanRead {
				get { return true; }
			}

			public override bool CanWrite {
				get { return false; }
			}

			//TODO: check!
			public override long Length {
				get { return end_pointer - start_pointer; }
			}

			//TODO: check!
			public override long Position {
				get { return pointer; }
				set {
					//TODO: is this correct?
					if (value > end_pointer)
						throw new ArgumentOutOfRangeException("value");

					Seek(value, SeekOrigin.Begin);
				}
			}

			public override long Seek(long offset, SeekOrigin origin) {
				if (origin == SeekOrigin.Begin && offset > end_pointer)
					return pointer;
				if (origin == SeekOrigin.Current && offset + pointer > end_pointer)
					return pointer;

				if (origin == SeekOrigin.End)
					throw new NotSupportedException();

				if (origin == SeekOrigin.Begin)
					pointer = offset;
				else
					pointer += offset;

				return pointer;
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override void Flush() {
			}

			public override void Write(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override int Read(byte[] buf, int off, int len) {
				// Is the end of the stream reached?
				if (pointer >= end_pointer) {
					return 0;
				}
				// How much can we Read?
				int read_count = System.Math.Min(len, (int)(end_pointer - pointer));
				int act_read_count = store.ReadByteArrayFrom(pointer, buf, off, read_count);
				if (act_read_count != read_count) {
					throw new IOException("act_read_count != read_count");
				}
				pointer += read_count;
				return read_count;
			}

			public override void Close() {
				// Do nothing
			}
		}



		private class StoreArea : IArea {

			protected const int BUFFER_SIZE = 8;

			protected readonly AbstractStore store;
			protected readonly long id;
			protected readonly long start_pointer;
			protected readonly long end_pointer;
			protected long m_position;
			// A small buffer used when accessing the underlying data
			protected readonly byte[] buffer = new byte[BUFFER_SIZE];

			public StoreArea(AbstractStore store, long id, long pointer) {
				this.store = store;
				// Check the pointer is within the bounds of the data area of the file
				store.CheckPointer(pointer);

				store.ReadByteArrayFrom(pointer, buffer, 0, 8);
				long v = ByteBuffer.ReadInt8(buffer, 0);
				if ((v & DeletedFlag /* 0x08000000000000000L */) != 0) {
					throw new IOException("Store being constructed on deleted area.");
				}

				long max_size = v - 16;
				this.id = id;
				this.start_pointer = pointer + 8;
				this.m_position = start_pointer;
				this.end_pointer = start_pointer + max_size;
			}

			public StoreArea(AbstractStore store, long id, long pointer, long fixed_size) {
				this.store = store;
				// Check the pointer is valid
				if (pointer != FixedAreaOffset) {
					store.CheckPointer(pointer);
				}

				this.id = id;
				this.start_pointer = pointer;
				this.m_position = start_pointer;
				this.end_pointer = start_pointer + fixed_size;
			}

			protected long CheckPositionBounds(int diff) {
				long new_pos = m_position + diff;
				if (new_pos > end_pointer) {
					throw new IOException("Position output of bounds. " +
										  " start=" + start_pointer +
										  " end=" + end_pointer +
										  " pos=" + m_position +
										  " new_pos=" + new_pos);
				}
				long old_pos = m_position;
				m_position = new_pos;
				return old_pos;
			}

			public long Id {
				get { return id; }
			}

			public int Position {
				get { return (int) (m_position - start_pointer); }
				set {
					long act_position = start_pointer + value;
					if (act_position >= 0 && act_position < end_pointer) {
						this.m_position = act_position;
						return;
					}
					throw new IOException("Moved position output of bounds.");
				}
			}

			public int Capacity {
				get { return (int) (end_pointer - start_pointer); }
			}

			public void CopyTo(IAreaWriter dest, int size) {
				// NOTE: Assuming 'destination' is a StoreArea, the temporary buffer
				// could be optimized away to a direct System.arraycopy.  However, this
				// function would need to be written as a lower level IO function.
				const int BUFFER_SIZE = 2048;
				byte[] buf = new byte[BUFFER_SIZE];
				int to_copy = System.Math.Min(size, BUFFER_SIZE);

				while (to_copy > 0) {
					Read(buf, 0, to_copy);
					dest.Write(buf, 0, to_copy);
					size -= to_copy;
					to_copy = System.Math.Min(size, BUFFER_SIZE);
				}
			}

			public byte ReadByte() {
				return (byte)store.ReadByteFrom(CheckPositionBounds(1));
			}

			public int Read(byte[] buf, int off, int len) {
				return store.ReadByteArrayFrom(CheckPositionBounds(len), buf, off, len);
			}

			public short ReadInt2() {
				store.ReadByteArrayFrom(CheckPositionBounds(2), buffer, 0, 2);
				return ByteBuffer.ReadInt2(buffer, 0);
			}

			public int ReadInt4() {
				store.ReadByteArrayFrom(CheckPositionBounds(4), buffer, 0, 4);
				return ByteBuffer.ReadInt4(buffer, 0);
			}

			public long ReadInt8() {
				store.ReadByteArrayFrom(CheckPositionBounds(8), buffer, 0, 8);
				return ByteBuffer.ReadInt8(buffer, 0);
			}

			public char ReadChar() {
				store.ReadByteArrayFrom(CheckPositionBounds(2), buffer, 0, 2);
				return ByteBuffer.ReadChar(buffer, 0);
			}



			public override String ToString() {
				return "[IArea start_pointer=" + start_pointer +
					   " end_pointer=" + end_pointer +
					   " position=" + m_position + "]";
			}

		}




		private class StoreMutableArea : StoreArea, IMutableArea {

			public StoreMutableArea(AbstractStore store, long id, long pointer)
				: base(store, id, pointer) {
			}

			public StoreMutableArea(AbstractStore store, long id, long pointer,
									long fixed_size)
				: base(store, id, pointer, fixed_size) {
			}

			public void CheckOut() {
				// Currently, no-op
			}

			public void WriteByte(byte b) {
				store.WriteByteTo(CheckPositionBounds(1), b);
			}

			public void Write(byte[] buf, int off, int len) {
				store.WriteByteArrayTo(CheckPositionBounds(len), buf, off, len);
			}

			public void Write(byte[] buf) {
				Write(buf, 0, buf.Length);
			}

			public void WriteInt2(short s) {
				ByteBuffer.WriteInt2(s, buffer, 0);
				store.WriteByteArrayTo(CheckPositionBounds(2), buffer, 0, 2);
			}

			public void WriteInt4(int i) {
				ByteBuffer.WriteInteger(i, buffer, 0);
				store.WriteByteArrayTo(CheckPositionBounds(4), buffer, 0, 4);
			}

			public void WriteInt8(long l) {
				ByteBuffer.WriteInt8(l, buffer, 0);
				store.WriteByteArrayTo(CheckPositionBounds(8), buffer, 0, 8);
			}

			public void WriteChar(char c) {
				ByteBuffer.WriteChar(c, buffer, 0);
				store.WriteByteArrayTo(CheckPositionBounds(2), buffer, 0, 2);
			}


			public override String ToString() {
				return "[MutableArea start_pointer=" + start_pointer +
					   " end_pointer=" + end_pointer +
					   " position=" + m_position + "]";
			}

		}

		/// <summary>
		/// A simple <see cref="Stream"/> implementation that is on top of an 
		/// <see cref="IAreaWriter"/> object.
		/// </summary>
		internal sealed class AreaOutputStream : Stream {

			private readonly IAreaWriter writer;

			public AreaOutputStream(IAreaWriter writer) {
				this.writer = writer;
			}

			public override void WriteByte(byte b) {
				writer.WriteByte(b);
			}

			public override bool CanRead {
				get { return false; }
			}

			public override bool CanSeek {
				get { return false; }
			}

			public override bool CanWrite {
				get { return true; }
			}

			public override long Length {
				get { return writer.Capacity; }
			}

			public override long Position {
				get { throw new NotSupportedException(); }
				set { throw new NotSupportedException(); }
			}

			public override long Seek(long offset, SeekOrigin origin) {
				throw new NotSupportedException();
			}

			public override void SetLength(long value) {
				throw new NotSupportedException();
			}

			public override int Read(byte[] buffer, int offset, int count) {
				throw new NotSupportedException();
			}

			public override void Write(byte[] buf, int off, int len) {
				writer.Write(buf, off, len);
			}

			public override void Flush() {
				// do nothing
			}

			public override void Close() {
				// do nothing
			}

		}



		private class StoreAreaWriter : StoreMutableArea, IAreaWriter {

			public StoreAreaWriter(AbstractStore store, long pointer, long fixed_size)
				: base(store, pointer, pointer + 8, fixed_size) {
			}

			public Stream GetOutputStream() {
				return new AreaOutputStream(this);
			}

			public void Finish() {
				// Currently, no-op
			}

		}





		// ---------- Static methods ----------

		/**
		 * The default bin sizes input bytes.  The minimum size of a bin is 32 and the
		 * maximum size is 2252832.
		 */

		private static readonly int[] BIN_SIZES =
			{
				32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448, 480,
				512, 544, 576, 608, 640, 672, 704, 736, 768, 800, 832, 864, 896, 928,
				960, 992, 1024, 1056, 1088, 1120, 1152, 1184, 1216, 1248, 1280, 1312,
				1344, 1376, 1408, 1440, 1472, 1504, 1536, 1568, 1600, 1632, 1664, 1696,
				1728, 1760, 1792, 1824, 1856, 1888, 1920, 1952, 1984, 2016, 2048, 2080,
				2144, 2208, 2272, 2336, 2400, 2464, 2528, 2592, 2656, 2720, 2784, 2848,
				2912, 2976, 3040, 3104, 3168, 3232, 3296, 3360, 3424, 3488, 3552, 3616,
				3680, 3744, 3808, 3872, 3936, 4000, 4064, 4128, 4384, 4640, 4896, 5152,
				5408, 5664, 5920, 6176, 6432, 6688, 6944, 7200, 7456, 7712, 7968, 8224,
				10272, 12320, 14368, 16416, 18464, 20512, 22560, 24608, 57376, 90144,
				122912, 155680, 1204256, 2252832
			};

		protected readonly static int BIN_ENTRIES = BIN_SIZES.Length;
		private readonly static int MAX_BIN_SIZE = BIN_SIZES[BIN_ENTRIES - 1];

	}
}