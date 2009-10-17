using System;
using System.IO;
using System.Net.Sockets;

namespace Deveel.Data.Util {
	/// <summary>
	/// Reads a command block on the underlying stream that is constrained by 
	/// a length marker preceeding the command.
	/// </summary>
	/// <remarks>
	/// This can be used as a hack work around for non-blocking IO because we 
	/// know ahead of time how much data makes up the next block of information 
	/// over the stream.
	/// </remarks>
	sealed class LengthMarkedBufferedInputStream : Stream {
		/// <summary>
		/// The initial buffer size of the internal input buffer.
		/// </summary>
		private const int INITIAL_BUFFER_SIZE = 512;

		/// <summary>
		/// The chained InputStream that is underneath this object.
		/// </summary>
		private IInputStream input;

		/**
		 * The buffer that is used to read in whatever is on the stream.
		 */
		private byte[] buf;

		/**
		 * The number of valid bytes in the buffer.
		 */
		private int count;

		/**
		 * The area of the buffer that is marked as being an available command.
		 * If it's -1 then there is no area marked.
		 */
		private int marked_length;

		/**
		 * The current index of the marked area that is being read.
		 */
		private int marked_index;

		/**
		 * The Constructor.
		 */
		public LengthMarkedBufferedInputStream(IInputStream input) {
			this.input = input;
			buf = new byte[INITIAL_BUFFER_SIZE];
			count = 0;
			marked_length = -1;
			marked_index = -1;
		}

		/**
		 * Ensures that the buffer is large enough to store the given value.  If
		 * it's not then it grows the buffer so it is big enough.
		 */
		private void EnsureCapacity(int new_size) {
			int old_size = buf.Length;
			if (new_size > old_size) {
				int cap = (old_size * 3) / 2 + 1;
				if (cap < new_size)
					cap = new_size;
				byte[] old_buf = buf;
				buf = new byte[cap];
				//      // Copy all the contents except the first 4 bytes (the size marker)
				//      System.arraycopy(old_buf, 4, buf, 4, count - 4);
				Array.Copy(old_buf, 0, buf, 0, count - 0);
			}
		}

		/**
		 * Private method, it is called when the end of the marked length is reached.
		 * It performs various maintenance operations to ensure the buffer
		 * consistency is maintained.
		 * Assumes we are calling from a synchronized method.
		 */
		private void HandleEndReached() {
			//    System.out.println();
			//    System.out.println("Shifting Buffer: ");
			//    System.out.println(" Index: " + marked_index +
			//                         ", Length: " + (count - marked_length));
			// Move anything from the end of the buffer to the start.
			Array.Copy(buf, marked_index, buf, 0, count - marked_length);
			count -= marked_length;

			// Reset the state
			marked_length = -1;
			marked_index = -1;
		}

		// ---------- Overwritten from FilterInputStream ----------

		public override int ReadByte() {
			lock (this) {
				if (marked_index == -1) {
					throw new IOException("No mark has been read yet.");
				}
				if (marked_index >= marked_length) {
					String debug_msg = "Read over end of length marked buffer.  ";
					debug_msg += "(marked_index=" + marked_index;
					debug_msg += ",marked_length=" + marked_length + ")";
					debug_msg += ")";
					throw new IOException(debug_msg);
				}
				int n = buf[marked_index++] & 0x0FF;
				if (marked_index >= marked_length) {
					HandleEndReached();
				}
				return n;
			}
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override bool CanRead {
			get { throw new NotImplementedException(); }
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
				if (marked_index == -1) {
					throw new IOException("No mark has been read yet.");
				}
				int read_upto = marked_index + len;
				if (read_upto > marked_length) {
					String debug_msg = "Read over end of length marked buffer.  ";
					debug_msg += "(marked_index=" + marked_index;
					debug_msg += ",len=" + len;
					debug_msg += ",marked_length=" + marked_length + ")";
					throw new IOException(debug_msg);
				}
				Array.Copy(buf, marked_index, b, off, len);
				marked_index = read_upto;
				if (marked_index >= marked_length) {
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
					if (marked_length >= 0) {
						return (marked_length - marked_index);
					}
					return 0;
				}
			}
		}

		// ---------- These methods aid in reading state from the stream ----------

		/**
		 * Checks to see if there is a complete command waiting on the input stream.
		 * Returns true if there is.  If this method returns true then it is safe
		 * to go ahead and process a single command from this stream.
		 * This will return true only once while there is a command pending until
		 * that command is completely read in.
		 * <p>
		 * 'max_size' is the maximum number of bytes we are allowing before an
		 * IOException is thrown.
		 */
		public bool PollForCommand(int max_size) {
			lock (this) {
				if (marked_length == -1) {
					int available = input.Available;
					//      System.out.print(available);
					//      System.out.print(", ");
					if (count > 0 || available > 0) {
						if ((count + available) > max_size) {
							throw new IOException("Marked length is greater than max size ( " +
												  (count + available) + " > " + max_size + " )");
						}

						EnsureCapacity(count + available);
						int read_in = input.Read(buf, count, available);

						//        System.out.println("-----");
						//        for (int i = 0; i < available; ++i) {
						//          System.out.print((char) buf[count + i] +
						//                           "(" + (int) buf[count + i] + "),");
						//        }
						//        System.out.println("-----");


						if (read_in == -1) {
							throw new EndOfStreamException();
						}
						count = count + read_in;

						//        else if (read_in != available) {
						//          throw new IOException("Read in size mismatch: " +
						//                        "read_in: " + read_in + " available: " + available);
						//        }

						// Check: Is a complete command available?
						if (count >= 4) {
							int length_marker = (((buf[0] & 0x0FF) << 24) +
												 ((buf[1] & 0x0FF) << 16) +
												 ((buf[2] & 0x0FF) << 8) +
												 ((buf[3] & 0x0FF) << 0));
							if (count >= length_marker + 4) {
								// Yes, complete command available.
								// mark this area up.
								marked_length = length_marker + 4;
								marked_index = 4;
								//            System.out.println("Complete command available: ");
								//            System.out.println("Length: " + marked_length +
								//                               ", Index: " + marked_index);
								return true;
							}
						}
					}
				}
				return false;
			}
		}

		/**
	   * Blocks until a complete command has been read in.
	   */
		public void blockForCommand() {
			lock (this) {
				while (true) {

					// Is there a command available?
					if (count >= 4) {
						int length_marker = (((buf[0] & 0x0FF) << 24) +
											 ((buf[1] & 0x0FF) << 16) +
											 ((buf[2] & 0x0FF) << 8) +
											 ((buf[3] & 0x0FF) << 0));
						if (count >= length_marker + 4) {
							// Yes, complete command available.
							// mark this area up.
							marked_length = length_marker + 4;
							marked_index = 4;
							//          System.out.println("marked_length = " + marked_length);
							//          System.out.println("marked_index = " + marked_index);
							return;
						}
					}

					// If the buffer is full grow it larger.
					if (count >= buf.Length) {
						EnsureCapacity(count + INITIAL_BUFFER_SIZE);
					}
					// Read in a block of data, block if nothing there
					int read_in = input.Read(buf, count, buf.Length - count);
					if (read_in == -1) {
						throw new EndOfStreamException();
					}
					count += read_in;
				}
			}
		}
	}
}