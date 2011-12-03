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
using System.IO;

using Deveel.Data.Util;

namespace Deveel.Data.Store {
	/// <summary>
	/// An implementation of the <see cref="IStore"/> interface that persists 
	/// information in the volatile heap memory.
	/// </summary>
	/// <remarks>
	/// Each <see cref="IArea"/> in the store is represented by a byte[] array from the heap.
	/// </remarks>
	public sealed class HeapStore : IStore {

		/// <summary>
		/// The fixed area element (a 64 byte area).
		/// </summary>
		private HeapAreaElement fixed_area_element;

		/// <summary>
		/// A hash map of area pointer to byte[] array that represents the area.
		/// </summary>
		private readonly HeapAreaElement[] area_map;

		/// <summary>
		/// A unique id key incremented for each new area created.
		/// </summary>
		private long unique_id_key;

		/// <summary>
		/// Creates the <see cref="HeapStore"/> with the given hash size.
		/// </summary>
		/// <param name="hashSize">The size of the hash for the heap store.</param>
		public HeapStore(int hashSize) {
			area_map = new HeapAreaElement[hashSize];
			unique_id_key = 0;
		}

		/// <summary>
		/// Creates the <see cref="HeapStore"/> with the default hash size (257).
		/// </summary>
		public HeapStore()
			: this(257) {
		}

		/// <summary>
		/// Searches the hash map and returns the area element for the given pointer.
		/// </summary>
		/// <param name="pointer"></param>
		/// <returns></returns>
		private HeapAreaElement GetAreaElement(long pointer) {
			lock (this) {
				// Find the pointer in the hash
				int hash_pos = (int)(pointer % area_map.Length);
				HeapAreaElement prev = null;
				HeapAreaElement element = area_map[hash_pos];
				// Search for this pointer
				while (element != null && element.Id != pointer) {
					prev = element;
					element = element.next_hash_element;
				}
				// If not found
				if (element == null) {
					throw new IOException("Pointer " + pointer + " is invalid.");
				}
				// Move the element to the start of the list.
				if (prev != null) {
					prev.next_hash_element = element.next_hash_element;
					element.next_hash_element = area_map[hash_pos];
					area_map[hash_pos] = element;
				}
				// Return the element
				return element;
			}
		}

		/// <summary>
		/// Returns a IMutableArea object for the fixed position.
		/// </summary>
		private HeapAreaElement FixedAreaElement {
			get {
				lock (this) {
					if (fixed_area_element == null)
						fixed_area_element = new HeapAreaElement(-1, 64);
					return fixed_area_element;
				}
			}
		}

		/// <summary>
		/// Returns the HeapAreaElement for the given pointer.
		/// </summary>
		/// <param name="pointer"></param>
		/// <returns></returns>
		private HeapAreaElement GetElement(long pointer) {
			if (pointer == -1)
				return FixedAreaElement;
			return GetAreaElement(pointer);
		}

		/// <inheritdoc/>
		public IAreaWriter CreateArea(long size) {
			if (size > Int32.MaxValue) {
				throw new IOException("'size' is too large.");
			}
			lock (this) {
				// Generate a unique id for this area.
				long id = unique_id_key;
				++unique_id_key;

				// Create the element.
				HeapAreaElement element = new HeapAreaElement(id, (int)size);
				// The position in the hash map
				int hash_pos = (int)(id % area_map.Length);
				// Add to the chain
				element.next_hash_element = area_map[hash_pos];
				// Set the element in the chain
				area_map[hash_pos] = element;
				// And return the object
				return element.GetAreaWriter();
			}
		}

		/// <inheritdoc/>
		public void DeleteArea(long pointer) {
			lock (this) {
				// Find the pointer in the hash
				int hash_pos = (int)(pointer % area_map.Length);
				HeapAreaElement prev = null;
				HeapAreaElement element = area_map[hash_pos];
				// Search for this pointer
				while (element != null && element.Id != pointer) {
					prev = element;
					element = element.next_hash_element;
				}
				// If not found
				if (element == null) {
					throw new IOException("Pointer " + pointer + " is invalid.");
				}
				// Remove
				if (prev == null) {
					area_map[hash_pos] = element.next_hash_element;
				} else {
					prev.next_hash_element = element.next_hash_element;
				}
				// Garbage collector should do the rest...
			}
		}

		/// <inheritdoc/>
		public Stream GetAreaInputStream(long pointer) {
			return GetElement(pointer).GetInputStream();
		}

		/// <inheritdoc/>
		public IArea GetArea(long pointer) {
			return GetElement(pointer).GetMutableArea();
		}

		/// <inheritdoc/>
		public IMutableArea GetMutableArea(long pointer) {
			return GetElement(pointer).GetMutableArea();
		}

		/// <inheritdoc/>
		public void LockForWrite() {
			// Not required
		}

		/// <inheritdoc/>
		public void UnlockForWrite() {
			// Not required
		}

		/// <inheritdoc/>
		public void CheckPoint() {
		}

		// ---------- Diagnostic ----------

		/// <inheritdoc/>
		public bool LastCloseClean() {
			// Close is not possible with a heap store, so always return true
			return true;
		}

		/// <inheritdoc/>
		public IList GetAllAreas() {
			throw new NotSupportedException("PENDING");
		}



		// ---------- Inner classes ----------

		/// <summary>
		/// An implementation of <see cref="IArea"/> for a byte array from 
		/// the heap.
		/// </summary>
		private class HeapArea : IMutableArea {
			/// <summary>
			/// The ID of this area.
			/// </summary>
			private readonly long id;

			/// <summary>
			/// A pointer to the byte[] array representing the entire area.
			/// </summary>
			private readonly byte[] heap_area;

			/// <summary>
			/// The start pointer in the heap area.
			/// </summary>
			private readonly int start_pointer;

			/// <summary>
			/// The current pointer into the area.
			/// </summary>
			private int position;

			/// <summary>
			/// The end pointer of the area.
			/// </summary>
			private readonly int end_pointer;

			protected internal HeapArea(long id, byte[] heap_area, int offset, int length) {
				this.id = id;
				this.heap_area = heap_area;
				start_pointer = offset;
				position = offset;
				end_pointer = offset + length;
			}

			private int CheckPositionBounds(int diff) {
				int new_pos = position + diff;
				if (new_pos > end_pointer) {
					throw new IOException("Position out of bounds. " +
										  " start=" + start_pointer +
										  " end=" + end_pointer +
										  " pos=" + position +
										  " new_pos=" + new_pos);
				}
				int old_pos = position;
				position = new_pos;
				return old_pos;
			}

			public long Id {
				get { return id; }
			}

			public int Position {
				get { return position - start_pointer; }
				set {
					int act_position = start_pointer + value;
					if (act_position >= 0 && act_position < end_pointer) {
						position = act_position;
						return;
					}
					throw new IOException("Moved position out of bounds.");
				}
			}

			public int Capacity {
				get { return end_pointer - start_pointer; }
			}

			public void CopyTo(IAreaWriter destination, int size) {
				const int BUFFER_SIZE = 2048;
				byte[] buf = new byte[BUFFER_SIZE];
				int to_copy = System.Math.Min(size, BUFFER_SIZE);

				while (to_copy > 0) {
					Read(buf, 0, to_copy);
					destination.Write(buf, 0, to_copy);
					size -= to_copy;
					to_copy = System.Math.Min(size, BUFFER_SIZE);
				}
			}

			public byte ReadByte() {
				return heap_area[CheckPositionBounds(1)];
			}

			public void WriteByte(byte b) {
				heap_area[CheckPositionBounds(1)] = b;
			}

			public int Read(byte[] buf, int off, int len) {
				Array.Copy(heap_area, CheckPositionBounds(len), buf, off, len);
				return len;
			}

			public void Write(byte[] buf, int off, int len) {
				Array.Copy(buf, off, heap_area, CheckPositionBounds(len), len);
			}

			public void Write(byte[] buf) {
				Write(buf, 0, buf.Length);
			}

			public short ReadInt2() {
				short s = ByteBuffer.ReadInt2(heap_area, CheckPositionBounds(2));
				return s;
			}

			public void WriteInt2(short s) {
				ByteBuffer.WriteInt2(s, heap_area, CheckPositionBounds(2));
			}

			public int ReadInt4() {
				int i = ByteBuffer.ReadInt4(heap_area, CheckPositionBounds(4));
				return i;
			}

			public void WriteInt4(int i) {
				ByteBuffer.WriteInteger(i, heap_area, CheckPositionBounds(4));
			}

			public long ReadInt8() {
				long l = ByteBuffer.ReadInt8(heap_area, CheckPositionBounds(8));
				return l;
			}

			public void WriteInt8(long l) {
				ByteBuffer.WriteInt8(l, heap_area, CheckPositionBounds(8));
			}

			public char ReadChar() {
				char c = ByteBuffer.ReadChar(heap_area, CheckPositionBounds(2));
				return c;
			}

			public void WriteChar(char c) {
				ByteBuffer.WriteChar(c, heap_area, CheckPositionBounds(2));
			}

			public void CheckOut() {
				// no-op
			}

			public override String ToString() {
				return "[IArea start_pointer=" + start_pointer +
					   " end_pointer=" + end_pointer +
					   " position=" + position + "]";
			}

		}

		private sealed class HeapAreaWriter : HeapArea, IAreaWriter {

			public HeapAreaWriter(long id, byte[] heap_area, int offset, int length)
				: base(id, heap_area, offset, length) {
			}

			public Stream GetOutputStream() {
				return new AbstractStore.AreaOutputStream(this);
			}

			public void Finish() {
				// Currently, no-op
			}

		}

		/// <summary>
		/// An area allocated from the heap store represented by a volatile 
		/// byte array.
		/// </summary>
		private sealed class HeapAreaElement {
			/// <summary>
			/// The id of this heap area (used as the hash key).
			/// </summary>
			private readonly long heap_id;
			/// <summary>
			/// A byte[] array that represents the volatile heap area.
			/// </summary>
			private readonly byte[] heap_area;
			/// <summary>
			/// The pointer to the next <see cref="HeapAreaElement"/> in this hash key.
			/// </summary>
			internal HeapAreaElement next_hash_element;

			internal HeapAreaElement(long heap_id, int area_size) {
				this.heap_id = heap_id;
				heap_area = new byte[area_size];
			}

			/// <summary>
			/// Returns the heap id for this element.
			/// </summary>
			public long Id {
				get { return heap_id; }
			}

			/// <summary>
			/// Returns a new <see cref="IAreaWriter"/> object for this element.
			/// </summary>
			/// <returns></returns>
			public IAreaWriter GetAreaWriter() {
				return new HeapAreaWriter(Id, heap_area, 0, heap_area.Length);
			}

			/// <summary>
			/// Returns a new IMutableArea object for this element.
			/// </summary>
			/// <returns></returns>
			public IMutableArea GetMutableArea() {
				return new HeapArea(Id, heap_area, 0, heap_area.Length);
			}

			/// <summary>
			/// Returns a new InputStream that is used to Read from the area.
			/// </summary>
			/// <returns></returns>
			public Stream GetInputStream() {
				return new MemoryStream(heap_area);
			}

/*
			private class MemoryInputStream : InputStream {
				public MemoryInputStream(byte [] buffer) {
					this.stream = new MemoryStream(buffer);
				}

				private readonly MemoryStream stream;

				#region Overrides of Stream

				public override long Seek(long offset, SeekOrigin origin) {
					return stream.Seek(offset, origin);
				}

				public override void SetLength(long value) {
					stream.SetLength(value);
				}

				public override int Read(byte[] buffer, int offset, int count) {
					return stream.Read(buffer, offset, count);
				}

				public override bool CanSeek {
					get { return stream.CanSeek; }
				}

				public override long Length {
					get { return stream.Length; }
				}

				public override long Position {
					get { return stream.Position; }
					set { stream.Position = value; }
				}

				#endregion
			}
*/
		}

	}
}