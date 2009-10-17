//  
//  NetworkInputStream.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
using System.IO;
using System.Net.Sockets;

namespace Deveel.Data.Util {
	class NetworkInputStream : Stream, IInputStream {
		public NetworkInputStream(Socket socket) {
			stream = new NISNetworkStream(socket);
		}

		private readonly NISNetworkStream stream;

		private class NISNetworkStream : NetworkStream {
			public NISNetworkStream(Socket socket)
				: base(socket, FileAccess.Read) {
			}

			public int Available {
				get { return Socket.Available; }
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