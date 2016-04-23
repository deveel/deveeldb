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
using System.Text;

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	public abstract class StoreBase : IStore {
		private long[] freeBinList;
		private long totalAllocatedSpace;

		private byte[] binArea = new byte[128 * 8];

		private static readonly int[] BinSizes =
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

		private readonly static int BinSizeEntries = BinSizes.Length;
		private readonly static int MaxBinSize = BinSizes[BinSizeEntries - 1];

		private const long ActiveFlag = Int64.MaxValue;
		private const long DeletedFlag = Int64.MinValue;

		private const long FixedAreaOffset = 128;

		// The offset into the file that the data areas start.
		private const long DataAreaOffset = 256 + 1024 + 32;

		private const long BinAreaOffset = 256;

		private const int Magic = 0x0AEAE91;

		protected StoreBase(bool isReadOnly) {
			freeBinList = new long[BinSizeEntries + 1];
			for (int i = 0; i < BinSizeEntries + 1; ++i) {
				freeBinList[i] = -1L;
			}

			WildernessOffset = -1;

			IsReadOnly = isReadOnly;
			IsClosed = true;
		}

		~StoreBase() {
			Dispose(false);
		}

		private long WildernessOffset { get; set; }

		public bool IsReadOnly { get; private set; }

		protected abstract long DataAreaEndOffset { get; }

		private void CheckOffset(long offset) {
			if (offset < DataAreaOffset || offset >= DataAreaEndOffset) {
				throw new IOException(String.Format("The offset is out of range ({0} > {1} > {2})", DataAreaOffset, offset,
					DataAreaEndOffset));
			}
		}

		private static int MinimumBinSizeIndex(long size) {
			int i = Array.BinarySearch(BinSizes, (int)size);
			if (i < 0) {
				i = -(i + 1);
			}
			return i;
		}

		private void ReadBins() {
			Read(BinAreaOffset, binArea, 0, 128 * 8);
			using (var bin = new MemoryStream(binArea)) {
				using (BinaryReader input = new BinaryReader(bin)) {
					for (int i = 0; i < 128; ++i) {
						freeBinList[i] = input.ReadInt64();
					}
				}
			}
		}

		private void WriteAllBins() {
			int p = 0;
			for (int i = 0; i < 128; ++i, p += 8) {
				long val = freeBinList[i];
				ByteBuffer.WriteInt8(val, binArea, p);
			}

			Write(BinAreaOffset, binArea, 0, 128 * 8);
		}

		private void WriteBinIndex(int index) {
			int p = index * 8;
			long val = freeBinList[index];
			ByteBuffer.WriteInt8(val, binArea, p);
			Write(BinAreaOffset + p, binArea, p, 8);
		}

		private void AddToBinChain(long pointer, long size) {
			CheckOffset(pointer);

			// What bin would this area fit into?
			int binChainIndex = MinimumBinSizeIndex(size);
			var headerInfo = new long[2];

			long curOffset = freeBinList[binChainIndex];

			if (curOffset == -1) {
				// If the bin chain has no elements,
				headerInfo[0] = (size | DeletedFlag);
				headerInfo[1] = -1;

				ReboundArea(pointer, headerInfo, true);
				freeBinList[binChainIndex] = pointer;

				WriteBinIndex(binChainIndex);
			} else {
				bool inserted = false;
				long lastOffset = -1;
				int searches = 0;
				while (curOffset != -1 && inserted == false) {
					// Get the current offset
					ReadAreaHeader(curOffset, headerInfo);

					long header = headerInfo[0];
					long next = headerInfo[1];

					// Assert - the header must have deleted flag
					if ((header & DeletedFlag) == 0)
						throw new IOException("Area not marked as deleted.");

					long areaSize = header ^ DeletedFlag;
					if (areaSize >= size || searches >= 12) {
						// Insert if the area size is >= than the size we are adding.
						// Set the previous header to point to this
						long previous = lastOffset;

						// Set up the deleted area
						headerInfo[0] = (size | DeletedFlag);
						headerInfo[1] = curOffset;

						ReboundArea(pointer, headerInfo, true);

						if (lastOffset != -1) {
							// Set the previous input the chain to point to the deleted area
							ReadAreaHeader(previous, headerInfo);

							headerInfo[1] = pointer;
							ReboundArea(previous, headerInfo, false);
						} else {
							// Otherwise set the head bin item
							freeBinList[binChainIndex] = pointer;
							WriteBinIndex(binChainIndex);
						}

						inserted = true;
					}

					lastOffset = curOffset;
					curOffset = next;
					++searches;
				}

				// If we reach the end and we haven't inserted,
				if (!inserted) {
					// Set the new deleted area.
					headerInfo[0] = (size | DeletedFlag);
					headerInfo[1] = -1;
					ReboundArea(pointer, headerInfo, true);

					// Set the previous entry to this
					ReadAreaHeader(lastOffset, headerInfo);
					headerInfo[1] = pointer;
					ReboundArea(lastOffset, headerInfo, false);
				}
			}
		}

		private void RemoveFromBinChain(long offset, long size) {
			// What bin index should we be looking input?
			int binChainIndex = MinimumBinSizeIndex(size);

			var prevOffset = -1L;
			var curOffset = freeBinList[binChainIndex];

			// Search this bin for the offset
			// NOTE: This is an iterative search through the bin chain
			while (offset != curOffset) {
				if (curOffset == -1)
					throw new IOException("Area not found input bin chain.");

				// Move to the next input the chain
				var headerInfo = new long[2];
				ReadAreaHeader(curOffset, headerInfo);

				prevOffset = curOffset;
				curOffset = headerInfo[1];
			}

			// Found the offset, so remove it,
			if (prevOffset == -1) {
				var headerInfo = new long[2];

				ReadAreaHeader(offset, headerInfo);
				freeBinList[binChainIndex] = headerInfo[1];
				WriteBinIndex(binChainIndex);
			} else {
				var headerInfo = new long[2];
				var headerInfo2 = new long[2];

				ReadAreaHeader(prevOffset, headerInfo2);
				ReadAreaHeader(offset, headerInfo);
				headerInfo2[1] = headerInfo[1];
				ReboundArea(prevOffset, headerInfo2, false);
			}

		}

		private void Free(long pointer) {
			// Get the area header
			var headerInfo = new long[2];
			ReadAreaHeader(pointer, headerInfo);

			if ((headerInfo[0] & DeletedFlag) != 0)
				throw new IOException("Area already marked as unallocated.");

			// If (pointer + size) reaches the end of the header area, set this as the
			// wilderness.
			bool setAsWilderness = ((pointer + headerInfo[0]) >= DataAreaEndOffset);

			var rOffset = pointer;
			var freeingAreaSize = headerInfo[0];
			var rSize = freeingAreaSize;

			// Can this area coalesce?
			var headerInfo2 = new long[2];
			var leftPointer = GetPreviousAreaHeader(pointer, headerInfo2);
			var coalesc = false;

			if ((headerInfo2[0] & DeletedFlag) != 0) {
				// Yes, we can coalesce left
				long areaSize = (headerInfo2[0] & ActiveFlag);

				rOffset = leftPointer;
				rSize = rSize + areaSize;

				// Remove left area from the bin
				RemoveFromBinChain(leftPointer, areaSize);
				coalesc = true;
			}

			if (!setAsWilderness) {
				long rightPointer = GetNextAreaHeader(pointer, headerInfo2);
				if ((headerInfo2[0] & DeletedFlag) != 0) {
					// Yes, we can coalesce right
					long areaSize = (headerInfo2[0] & ActiveFlag);

					rSize = rSize + areaSize;

					// Remove right from the bin
					RemoveFromBinChain(rightPointer, areaSize);
					setAsWilderness = (rightPointer == WildernessOffset);
					coalesc = true;
				}
			}

			// If we are coalescing parent areas
			if (coalesc)
				CoalesceArea(rOffset, rSize);

			// Add this new area to the bin chain,
			AddToBinChain(rOffset, rSize);

			// Do we set this as the wilderness?
			if (setAsWilderness)
				WildernessOffset = rOffset;

			totalAllocatedSpace -= freeingAreaSize;
		}


		private long GetPreviousAreaHeader(long offset, long[] header) {
			// If the offset is the start of the file area
			if (offset == DataAreaOffset) {
				// Return a 0 sized block
				header[0] = 0;
				return -1;
			}

			Read(offset - 8, headerBuf, 0, 8);
			long sz = ByteBuffer.ReadInt8(headerBuf, 0);
			sz = sz & ActiveFlag;
			long previousPointer = offset - sz;
			Read(previousPointer, headerBuf, 0, 8);
			header[0] = ByteBuffer.ReadInt8(headerBuf, 0);
			return previousPointer;
		}

		private long GetNextAreaHeader(long offset, long[] header) {
			Read(offset, headerBuf, 0, 8);
			long sz = ByteBuffer.ReadInt8(headerBuf, 0);
			sz = sz & ActiveFlag;
			long nextOffset = offset + sz;

			if (nextOffset >= DataAreaEndOffset) {
				// Return a 0 sized block
				header[0] = 0;
				return -1;
			}

			Read(nextOffset, headerBuf, 0, 8);
			header[0] = ByteBuffer.ReadInt8(headerBuf, 0);
			return nextOffset;
		}

		protected void ReadAreaHeader(long offset, long[] header) {
			Read(offset, headerBuf, 0, 16);
			header[0] = ByteBuffer.ReadInt8(headerBuf, 0);
			header[1] = ByteBuffer.ReadInt8(headerBuf, 8);
		}

		private readonly byte[] headerBuf = new byte[16];

		private void ReboundArea(long offset, long[] header, bool writeHeaders) {
			if (writeHeaders) {
				ByteBuffer.WriteInt8(header[0], headerBuf, 0);
				ByteBuffer.WriteInt8(header[1], headerBuf, 8);
				Write(offset, headerBuf, 0, 16);
			} else {
				ByteBuffer.WriteInt8(header[1], headerBuf, 8);
				Write(offset + 8, headerBuf, 8, 8);
			}
		}

		private void CoalesceArea(long offset, long size) {
			ByteBuffer.WriteInt8(size, headerBuf, 0);

			// ISSUE: Boundary alteration is a moment when corruption could occur.
			//   There are two seeks and writes here and when we are setting the
			//   end points, there is a risk of failure.

			Write(offset, headerBuf, 0, 8);
			Write((offset + size) - 8, headerBuf, 0, 8);
		}

		private void CropArea(long offset, long allocatedSize) {
			// Get the header info
			var headerInfo = new long[2];
			ReadAreaHeader(offset, headerInfo);

			var header = headerInfo[0];

			var freeAreaSize = header;
			var sizeDifference = freeAreaSize - allocatedSize;
			var isWilderness = (offset == WildernessOffset);

			// If the difference is greater than 512 bytes, add the excess space to
			// a free bin.

			if ((isWilderness && sizeDifference >= 32) || sizeDifference >= 512) {
				// Split the area into two areas.
				SplitArea(offset, allocatedSize);

				long leftOverPointer = offset + allocatedSize;
				// Add this area to the bin chain
				AddToBinChain(leftOverPointer, sizeDifference);

				// If offset is the wilderness area, set this as the new wilderness
				if (isWilderness ||
					(leftOverPointer + sizeDifference) >= DataAreaEndOffset) {
					WildernessOffset = leftOverPointer;
				}

			} else {
				// If offset is the wilderness area, set wilderness to -1
				if (isWilderness) {
					WildernessOffset = -1;
				}
			}
		}

		private long Alloc(long size) {
			if (size < 0)
				throw new IOException("Negative size allocation");

			// Add 16 bytes for headers
			size = size + 16;

			// If size < 32, make size = 32
			if (size < 32)
				size = 32;

			// Round all sizes up to the nearest 8
			long d = size & 0x07L;
			if (d != 0)
				size = size + (8 - d);

			long realAllocSize = size;

			// Search the free bin list for the first bin that matches the given size.
			int binChainIndex;
			if (size > MaxBinSize) {
				binChainIndex = BinSizeEntries;
			} else {
				int i = MinimumBinSizeIndex(size);
				binChainIndex = i;
			}

			// Search the bins until we find the first area that is the nearest fit to
			// the size requested.
			int foundBinIndex = -1;
			long prevOffset = -1;
			bool first = true;
			for (int i = binChainIndex;
				i < BinSizeEntries + 1 && foundBinIndex == -1;
				++i) {
				long curOffset = freeBinList[i];
				if (curOffset != -1) {
					if (!first) {
						// Pick this..
						foundBinIndex = i;
						prevOffset = -1;
					} else {
						// Search this bin for the first that's big enough.
						// We only search the first 12 entries input the bin before giving up.

						long lastOffset = -1;
						int searches = 0;
						while (curOffset != -1 &&
						       foundBinIndex == -1 &&
						       searches < 12) {

							var headerInfo = new long[2];
							ReadAreaHeader(curOffset, headerInfo);

							long areaSize = (headerInfo[0] & ActiveFlag);

							// Is this area is greater or equal than the required size
							// and is not the wilderness area, pick it.
							if (curOffset != WildernessOffset && areaSize >= size) {
								foundBinIndex = i;
								prevOffset = lastOffset;
							}

							// Go to next input chain.
							lastOffset = curOffset;
							curOffset = headerInfo[1];
							++searches;
						}
					}

				}

				first = false;
			}

			// If no area can be recycled,
			if (foundBinIndex == -1) {
				// Allocate a new area of the given size.
				// If there is a wilderness, grow the wilderness area to the new size,
				long workingOffset;
				long sizeToGrow;
				long currentAreaSize;
				if (WildernessOffset != -1) {
					workingOffset = WildernessOffset;

					var headerInfo = new long[2];
					ReadAreaHeader(WildernessOffset, headerInfo);

					long wildernessSize = (headerInfo[0] & ActiveFlag);

					// Remove this from the bins
					RemoveFromBinChain(workingOffset, wildernessSize);

					// For safety, we set wilderness_pointer to -1
					WildernessOffset = -1;
					sizeToGrow = size - wildernessSize;
					currentAreaSize = wildernessSize;
				} else {
					// wilderness_pointer == -1 so add to the end of the data area.
					workingOffset = DataAreaEndOffset;
					sizeToGrow = size;
					currentAreaSize = 0;
				}

				long expandedSize = 0;
				if (sizeToGrow > 0) {
					// Expand the data area to the new size.
					expandedSize = ExpandDataArea(sizeToGrow);
				}

				// Coalesce the new area to the given size
				CoalesceArea(workingOffset, currentAreaSize + expandedSize);

				// crop the area
				CropArea(workingOffset, size);

				// Add to the total allocated space
				totalAllocatedSpace += realAllocSize;

				return workingOffset;
			} else {
				// An area is taken from the bins,
				long freeAreaOffset;
				var headerInfo = new long[2];

				// Remove this area from the bin chain and possibly add any excess space
				// left over to a new bin.
				if (prevOffset == -1) {
					freeAreaOffset = freeBinList[foundBinIndex];
					ReadAreaHeader(freeAreaOffset, headerInfo);
					freeBinList[foundBinIndex] = headerInfo[1];
					WriteBinIndex(foundBinIndex);
				} else {
					var headerInfo2 = new long[2];
					ReadAreaHeader(prevOffset, headerInfo2);
					freeAreaOffset = headerInfo2[1];
					ReadAreaHeader(freeAreaOffset, headerInfo);
					headerInfo2[1] = headerInfo[1];
					ReboundArea(prevOffset, headerInfo2, false);
				}

				// Reset the header of the recycled area.
				headerInfo[0] = (headerInfo[0] & ActiveFlag);
				ReboundArea(freeAreaOffset, headerInfo, true);

				// Crop the area to the given size.
				CropArea(freeAreaOffset, size);

				// Add to the total allocated space
				totalAllocatedSpace += realAllocSize;

				return freeAreaOffset;
			}
		}


		private long ExpandDataArea(long minSize) {
			long endOfDataArea = DataAreaEndOffset;

			// Round all sizes up to the nearest 8
			// We grow only by a small amount if the area is small, and a large amount
			// if the area is large.
			long overGrow = endOfDataArea / 64;
			long d = (overGrow & 0x07L);
			if (d != 0)
				overGrow = overGrow + (8 - d);

			overGrow = System.Math.Min(overGrow, 262144L);
			if (overGrow < 1024)
				overGrow = 1024;

			long growBy = minSize + overGrow;
			long newFileLength = endOfDataArea + growBy;
			SetDataAreaSize(newFileLength);
			return growBy;
		}

		protected void SplitArea(long offset, long newBoundary) {
			// Split the area pointed to by the offset.
			Read(offset, headerBuf, 0, 8);
			long curSize = ByteBuffer.ReadInt8(headerBuf, 0) & ActiveFlag;
			long leftSize = newBoundary;
			long rightSize = curSize - newBoundary;

			if (rightSize < 0)
				throw new IOException("Could not split the area.");

			ByteBuffer.WriteInt8(leftSize, headerBuf, 0);
			ByteBuffer.WriteInt8(rightSize, headerBuf, 8);

			// ISSUE: Boundary alteration is a moment when corruption could occur.
			//   There are three seeks and writes here and when we are setting the
			//   end points, there is a risk of failure.

			// First set the boundary
			Write((offset + newBoundary) - 8, headerBuf, 0, 16);
			// Now set the end points
			Write(offset, headerBuf, 0, 8);
			Write((offset + curSize) - 8, headerBuf, 8, 8);
		}

		private static bool IsValidBoundarySize(long size) {
			const long maxAreaSize = (long)Int32.MaxValue * 200;
			size = size & ActiveFlag;
			return ((size < maxAreaSize) && (size >= 24) && ((size & 0x07) == 0));
		}

		private void Init() {
			lock (this) {
				SetDataAreaSize(DataAreaOffset);

				using (var stream = new MemoryStream((int) BinAreaOffset)) {
					using (BinaryWriter writer = new BinaryWriter(stream, Encoding.Unicode)) {

						// The file MAGIC
						writer.Write(Magic); // 0

						// The file version
						writer.Write(1); // 4

						// The number of areas (chunks) input the file (currently unused)
						writer.Write(-1L); // 8

						// File open/close status byte
						writer.Write((byte) 0); // 16

						writer.Flush();

						byte[] buffer = new byte[(int) DataAreaOffset];
						byte[] temp = stream.ToArray();
						Array.Copy(temp, 0, buffer, 0, temp.Length);
						
						for (int i = (int) BinAreaOffset; i < (int) DataAreaOffset; ++i) {
							buffer[i] = 255;
						}

						Write(0, buffer, 0, buffer.Length);
					}
				}
			}
		}


		protected abstract void SetDataAreaSize(long length);

		public bool Open() {
			lock (this) {
				OpenStore(IsReadOnly);

				// If it's small, initialize to empty
				if (DataAreaEndOffset < DataAreaOffset)
					Init();

				byte[] readBuf = new byte[(int) BinAreaOffset];
				Read(0, readBuf, 0, readBuf.Length);

				using (var stream = new MemoryStream(readBuf)) {
					using (var reader = new BinaryReader(stream)) {

						int magic = reader.ReadInt32();
						if (magic != Magic)
							throw new IOException("Format invalid: Magic value is not as expected.");

						int version = reader.ReadInt32();
						if (version != 1)
							throw new IOException("Format invalid: unrecognized version.");

						reader.ReadInt64(); // ignore
						byte status = reader.ReadByte();
						ClosedClean = true;

						if (status == 1) {
							// This means the store wasn't closed cleanly.
							ClosedClean = false;
						}
					}
				}

				// Read the bins
				ReadBins();

				// Mark the file as open
				if (!IsReadOnly)
					Write(16, 1);

				long fileLength = DataAreaEndOffset;
				if (fileLength <= 8) {
					throw new IOException("Format invalid: File size is too small.");
				}

				// Set the wilderness offset.
				if (fileLength == DataAreaOffset) {
					WildernessOffset = -1;
				} else {
					Read(fileLength - 8, readBuf, 0, 8);
					long lastBoundary = ByteBuffer.ReadInt8(readBuf, 0);
					long lastAreaPointer = fileLength - lastBoundary;

					if (lastAreaPointer < DataAreaOffset)
						throw new IOException("File corrupt: last area offset is before data part of file.");

					if (lastAreaPointer > fileLength - 8)
						throw new IOException("File corrupt: last_area_pointer at the end of the file.");

					Read(lastAreaPointer, readBuf, 0, 8);

					long lastAreaHeader = ByteBuffer.ReadInt8(readBuf, 0);

					// If this is a freed block, then set this are the wilderness offset.
					if ((lastAreaHeader & DeletedFlag) != 0) {
						WildernessOffset = lastAreaPointer;
					} else {
						WildernessOffset = -1;
					}
				}

				return ClosedClean;
			}
		}

		public bool IsClosed { get; set; }

		public void Close() {
			lock (this) {
				// Mark the file as closed
				if (!IsReadOnly)
					Write(16, 0);

				try {
					CloseStore();
				} finally {
					IsClosed = true;
				}
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private bool disposed;

		protected virtual void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					if (binArea != null)
						Array.Resize(ref binArea, 0);

					if (!IsClosed)
						Close();
				}

				binArea = null;
				disposed = true;
			}
		}

		public IArea CreateArea(long size) {
			lock (this) {
				long pointer = Alloc(size);
				return new StoreArea(this, pointer, pointer + 8, false, size);
			}
		}

		public void DeleteArea(long id) {
			lock (this) {
				Free(id);
			}
		}

		public IArea GetArea(long id, bool readOnly) {
			// If this is the fixed area
			if (id == -1)
				return new StoreArea(this, id, FixedAreaOffset, readOnly, 64);

			// Otherwise must be a regular area
			return new StoreArea(this, id, id, readOnly);
		}

		public abstract void Lock();

		public abstract void Unlock();

		//public abstract void CheckPoint();

		public bool ClosedClean { get; private set; }

		public IEnumerable<long> GetAllAreas() {
			var list = new List<long>();

			long endOfDataArea = DataAreaEndOffset;

			long[] header = new long[2];
			// The first header
			long offset = DataAreaOffset;
			while (offset < endOfDataArea) {
				ReadAreaHeader(offset, header);

				long areaSize = (header[0] & ActiveFlag);
				if ((header[0] & DeletedFlag) == 0)
					list.Add(offset);

				offset += areaSize;
			}

			return list;
		}

		internal IEnumerable<long> FindAllocatedAreasNotIn(List<long> usedAreas) {
			// Sort the list
			var list = new List<long>(usedAreas);
			list.Sort();

			// The list of leaked areas
			var leakedAreas = new List<long>();

			int listIndex = 0;

			// What area are we looking for?
			long lookingFor = Int64.MaxValue;
			if (listIndex < list.Count) {
				lookingFor = list[listIndex];
				++listIndex;
			}

			long endOfDataArea = DataAreaEndOffset;
			long[] header = new long[2];

			long offset = DataAreaOffset;
			while (offset < endOfDataArea) {
				ReadAreaHeader(offset, header);

				long areaSize = (header[0] & ActiveFlag);
				bool areaFree = (header[0] & DeletedFlag) != 0;

				if (offset == lookingFor) {
					if (areaFree)
						throw new IOException("Area is not allocated!");

					// Update the 'looking_for' offset
					if (listIndex < list.Count) {
						lookingFor = (long)list[listIndex];
						++listIndex;
					} else {
						lookingFor = Int64.MaxValue;
					}
				} else if (offset > lookingFor) {
					throw new IOException("IArea (offset = " + lookingFor + ") wasn't found input store!");
				} else {
					// An area that isn't input the list
					if (!areaFree) {
						// This is a leaked area.
						// It isn't free and it isn't input the list
						leakedAreas.Add(offset);
					}
				}

				offset += areaSize;
			}

			return leakedAreas.ToArray();
		}

		protected abstract void OpenStore(bool readOnly);

		protected abstract void CloseStore();

		protected int ReadByte(long offset) {
			var buffer = new byte[1];
			var count = Read(offset, buffer, 0, 1);
			if (count == 0)
				return -1;

			return buffer[0];
		}

		protected abstract int Read(long offset, byte[] buffer, int index, int length);

		protected void Write(long offset, byte value) {
			Write(offset, new []{value}, 0, 1);
		}

		protected abstract void Write(long offset, byte[] buffer, int index, int length);

		#region StoreArea

		/// <summary>
		/// An <see cref="IArea"/> that is backed by a <see cref="StoreBase"/>.
		/// </summary>
		class StoreArea : IArea {
			private byte[] buffer = new byte[BufferSize];
			private long position;

			private const int BufferSize = 8;

			public StoreArea(StoreBase store, long id, long offset, bool readOnly) {
				Store = store;
				Id = id;
				IsReadOnly = readOnly;

				store.CheckOffset(offset);

				store.Read(offset, buffer, 0, 8);
				long v = ByteBuffer.ReadInt8(buffer, 0);
				if ((v & DeletedFlag) != 0)
					throw new IOException("Store being constructed on deleted area.");

				long maxSize = v - 16;
				StartOffset = offset + 8;
				position = StartOffset;
				EndOffset = StartOffset + maxSize;
			}

			public StoreArea(StoreBase store, long id, long offset, bool readOnly, long fixedSize) {
				Store = store;
				Id = id;
				IsReadOnly = readOnly;

				// Check the offset is valid
				if (offset != FixedAreaOffset) {
					store.CheckOffset(offset);
				}

				StartOffset = offset;
				position = StartOffset;
				EndOffset = StartOffset + fixedSize;
			}

			~StoreArea() {
				Dispose(false);
			}

			public long Id { get; private set; }

			private long StartOffset { get; set; }

			private long EndOffset { get; set; }

			private StoreBase Store { get; set; }

			public virtual bool IsReadOnly { get; private set; }

			public long Position {
				get { return position - StartOffset; }
				set {
					long actPosition = StartOffset + value;
					if (actPosition < 0 || actPosition >= EndOffset)
						throw new IOException("Moved position out of the area bounds.");

					position = actPosition;
				}
			}

			public int Length {
				get { return (int)(EndOffset - StartOffset); }
			}

			protected long CheckAreaOffset(int diff) {
				long newPos = position + diff;
				if (newPos > EndOffset)
					throw new IOException("Trying to access a position out of area bounds.");
				
				long oldPos = position;
				position = newPos;
				return oldPos;
			}

			public void CopyTo(IArea destArea, int size) {
				// NOTE: Assuming 'destination' is a StoreArea, the temporary buffer
				// could be optimized away to a direct Array.Ccopy.  However, this
				// function would need to be written as a lower level IO function.
				const int bufferSize = 2048;
				byte[] buf = new byte[bufferSize];
				int toCopy = System.Math.Min(size, bufferSize);

				while (toCopy > 0) {
					var read = Read(buf, 0, toCopy);
					if (read == 0)
						break;

					destArea.Write(buf, 0, read);
					size -= toCopy;
					toCopy = System.Math.Min(size, bufferSize);
				}
			}

			protected virtual void Dispose(bool disposing) {
				if (disposing) {
					// TODO:
				}

				buffer = null;
				Store = null;
			}

			public void Dispose() {
				Dispose(true);
				GC.SuppressFinalize(this);
			}

			public int Read(byte[] buffer, int offset, int length) {
				return Store.Read(CheckAreaOffset(length), buffer, offset, length);
			}

			public void Write(byte[] buffer, int offset, int length) {
				if (IsReadOnly)
					throw new IOException("The area is read-only access.");

				Store.Write(CheckAreaOffset(length), buffer, offset, length);
			}

			public void Flush() {
			}
		}

		#endregion
	}
}