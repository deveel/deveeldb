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
using System.Net.Sockets;

namespace Deveel.Data.Protocol {
	public class NetworkInputStream : Stream, IInputStream {
		public NetworkInputStream(Socket socket) {
			stream = new NISNetworkStream(socket);
		}

		private readonly NISNetworkStream stream;

		private class NISNetworkStream : NetworkStream {
			public NISNetworkStream(Socket socket)
				: base(socket, FileAccess.Read) {
			}

			public int Available {
				get { return (Socket.Connected ? Socket.Available : 0); }
			}

			public override int Read(byte[] buffer, int offset, int size) {
				if (!Socket.Connected)
					return 0;

				return base.Read(buffer, offset, size);
			}
		}

		public int Available {
			get { return stream.Available; }
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanWrite {
			get { return false; }
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotSupportedException();
		}

		public override void Flush() {
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			return stream.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count) {
			throw new NotSupportedException();
		}

		public override bool CanSeek {
			get { return false; }
		}

		public override long Length {
			get { throw new NotSupportedException(); }
		}

		public override long Position {
			get { throw new NotSupportedException(); }
			set { throw new NotSupportedException(); }
		}
	}
}