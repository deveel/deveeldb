using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data.Store {
	public class InMemoryStore : IStore {
		public IArea CreateArea(long size) {
			throw new NotImplementedException();
		}

		public void DeleteArea(long id) {
			throw new NotImplementedException();
		}

		public Stream GetAreaInputStream(long id) {
			throw new NotImplementedException();
		}

		public IArea GetArea(long id) {
			throw new NotImplementedException();
		}

		public void LockForWrite() {
			throw new NotImplementedException();
		}

		public void UnlockForWrite() {
			throw new NotImplementedException();
		}

		public void CheckPoint() {
			throw new NotImplementedException();
		}

		public bool LastCloseClean() {
			throw new NotImplementedException();
		}

		public IEnumerable<long> GetAllAreas() {
			throw new NotImplementedException();
		}

		#region InMemoryArea

		class InMemoryArea : IArea {
			private byte[] data;
			private int position;
			private int length;
			private int startPosition;
			private int endPosition;

			public InMemoryArea(long id, byte[] data, int offset, int length) {
				this.data = data;
				this.length = length;

				position = startPosition = offset;
				endPosition = offset + length;

				Id = id;
			}

			public long Id { get; private set; }

			public bool IsReadOnly { get; private set; }

			public int Position {
				get { return position; }
				set {
					int actPosition = startPosition + value;
					if (actPosition < 0 || actPosition >= endPosition)
						throw new IOException("Moved position out of bounds.");

					position = actPosition;
				}
			}

			public int Capacity {
				get { return endPosition - startPosition; }
			}

			public int Length {
				get { return length; }
			}

			private int CheckPositionBounds(int diff) {
				int newPos = position + diff;
				if (newPos > endPosition) {
					throw new IOException("Position out of bounds. " +
										  " start=" + startPosition +
										  " end=" + endPosition +
										  " pos=" + position +
										  " new_pos=" + newPos);
				}

				int oldPos = position;
				position = newPos;
				return oldPos;
			}

			public void CopyTo(IArea destArea, int size) {
				throw new NotImplementedException();
			}

			public int Read(byte[] buffer, int offset, int length) {
				Array.Copy(data, CheckPositionBounds(length), buffer, offset, length);
				return length;
			}

			public void Write(byte[] buffer, int offset, int length) {
				Array.Copy(buffer, offset, data, CheckPositionBounds(length), length);
			}

			public void Flush() {
			}
		}

		#endregion
	}
}
