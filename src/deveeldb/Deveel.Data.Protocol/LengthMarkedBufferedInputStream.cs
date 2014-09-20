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

using Deveel.Data.Util;

namespace Deveel.Data.Protocol {
	/// <summary>
	/// Reads a command block on the underlying stream that is constrained by 
	/// a length marker preceeding the command.
	/// </summary>
	/// <remarks>
	/// This can be used as a hack work around for non-blocking IO because we 
	/// know ahead of time how much data makes up the next block of information 
	/// over the stream.
	/// </remarks>
	public sealed class LengthMarkedBufferedInputStream : Stream {
		/// <summary>
		/// The initial buffer size of the internal input buffer.
		/// </summary>
		private const int InitialBufferSize = 512;

		/// <summary>
		/// The chained InputStream that is underneath this object.
		/// </summary>
		private readonly IInputStream input;

		/// <summary>
		/// The buffer that is used to read in whatever is on the stream.
		/// </summary>
		private byte[] buf;

		/// <summary>
		/// The number of valid bytes in the buffer.
		/// </summary>
		private int count;

		/// <summary>
		/// The area of the buffer that is marked as being an available command.
		/// If it's -1 then there is no area marked.
		/// </summary>
		private int markedLength;

		/// <summary>
		/// The current index of the marked area that is being read.
		/// </summary>
		private int markedIndex;

		public LengthMarkedBufferedInputStream(IInputStream input) {
			this.input = input;
			buf = new byte[InitialBufferSize];
			count = 0;
			markedLength = -1;
			markedIndex = -1;
		}

		/// <summary>
		/// Ensures that the buffer is large enough to store the given value.
		/// </summary>
		/// <param name="newSize"></param>
		/// <remarks>
		/// If the buffer is not large enough this method grows it so it is big enough.
		/// </remarks>
		private void EnsureCapacity(int newSize) {
			int old_size = buf.Length;
			if (newSize > old_size) {
				int cap = (old_size * 3) / 2 + 1;
				if (cap < newSize)
					cap = newSize;
				byte[] oldBuf = buf;
				buf = new byte[cap];
				//      // Copy all the contents except the first 4 bytes (the size marker)
				Array.Copy(oldBuf, 0, buf, 0, count);
			}
		}

		/// <summary>
		/// Called when the end of the marked length is reached.
		/// </summary>
		/// <remarks>
		/// It performs various maintenance operations to ensure the buffer consistency 
		/// is maintained.
		/// <para>
		/// Assumes we are calling from a synchronized method.
		/// </para>
		/// </remarks>
		private void HandleEndReached() {
			// Move anything from the end of the buffer to the start.
			Array.Copy(buf, markedIndex, buf, 0, count - markedLength);
			count -= markedLength;

			// Reset the state
			markedLength = -1;
			markedIndex = -1;
		}

		// ---------- Overwritten from Stream ----------

		public override int ReadByte() {
			lock (this) {
				if (markedIndex == -1)
					throw new IOException("No mark has been read yet.");

				if (markedIndex >= markedLength) {
					string debugMsg = "Read over end of length marked buffer.  ";
					debugMsg += "(marked_index=" + markedIndex;
					debugMsg += ",marked_length=" + markedLength + ")";
					debugMsg += ")";
					throw new IOException(debugMsg);
				}
				int n = buf[markedIndex++] & 0x0FF;
				if (markedIndex >= markedLength) {
					HandleEndReached();
				}
				return n;
			}
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return false; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		public override long Length {
			get { throw new NotImplementedException(); }
		}

		public override long Position {
			get { throw new NotImplementedException(); }
			set { throw new NotImplementedException(); }
		}

		public override void Flush() {
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotSupportedException();
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] b, int off, int len) {
			lock (this) {
				if (markedIndex == -1)
					throw new IOException("No mark has been read yet.");

				int readUpto = markedIndex + len;
				if (readUpto > markedLength) {
					String debug_msg = "Read over end of length marked buffer.  ";
					debug_msg += "(marked_index=" + markedIndex;
					debug_msg += ",len=" + len;
					debug_msg += ",marked_length=" + markedLength + ")";
					throw new IOException(debug_msg);
				}
				Array.Copy(buf, markedIndex, b, off, len);
				markedIndex = readUpto;
				if (markedIndex >= markedLength) {
					HandleEndReached();
				}
				return len;
			}
		}

		public int Available {
			get {
				lock (this) {
					// This method only returns a non 0 value if there is a complete command
					// waiting on the stream.
					if (markedLength >= 0) {
						return (markedLength - markedIndex);
					}
					return 0;
				}
			}
		}

		// ---------- These methods aid in reading state from the stream ----------

		/// <summary>
		/// Checks to see if there is a complete command waiting on the input stream.
		/// </summary>
		/// <param name="maxSize">The maximum number of bytes we are allowing before an 
		/// <see cref="IOException"/> is thrown.</param>
		/// <remarks>
		/// If this method returns true then it is safe to go ahead and process a single 
		/// command from this stream. This will return true only once while there is a 
		/// command pending until that command is completely read in.
		/// </remarks>
		/// <returns>
		/// Returns true if there is a complete command.
		/// </returns>
		public bool PollForCommand(int maxSize) {
			lock (this) {
				if (markedLength == -1) {
					int available = input.Available;
					if (count > 0 || available > 0) {
						if ((count + available) > maxSize) {
							throw new IOException("Marked length is greater than max size ( " +
												  (count + available) + " > " + maxSize + " )");
						}

						EnsureCapacity(count + available);
						int readIn = input.Read(buf, count, available);

						if (readIn == 0) {
							//TODO: Check this format...
							// throw new EndOfStreamException();

							// zero bytes read means that the stream is finished...
							return false;
						}
						count = count + readIn;

						// Check: Is a complete command available?
						if (count >= 4) {
							int lengthMarker = ByteBuffer.ReadInt4(buf, 0);

							if (count >= lengthMarker + 4) {
								// Yes, complete command available.
								// mark this area up.
								markedLength = lengthMarker + 4;
								markedIndex = 4;
								return true;
							}
						}
					}
				}
				return false;
			}
		}

		/// <summary>
		/// Blocks until a complete command has been read in.
		/// </summary>
		public void BlockForCommand() {
			lock (this) {
				while (true) {
					// Is there a command available?
					if (count >= 4) {
						int lengthMarker = ByteBuffer.ReadInt4(buf, 0);
						if (count >= lengthMarker + 4) {
							// Yes, complete command available.
							// mark this area up.
							markedLength = lengthMarker + 4;
							markedIndex = 4;
							return;
						}
					}

					// If the buffer is full grow it larger.
					if (count >= buf.Length) {
						EnsureCapacity(count + InitialBufferSize);
					}
					// Read in a block of data, block if nothing there
					int read_in = input.Read(buf, count, buf.Length - count);
					if (read_in == 0) {
						//TODO: Check this format...
						// throw new EndOfStreamException();

						// zero bytes read means that the stream is finished...
						return;
					}
					count += read_in;
				}
			}
		}
	}
}