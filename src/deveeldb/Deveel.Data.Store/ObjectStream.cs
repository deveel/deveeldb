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

namespace Deveel.Data.Store {
	public sealed class ObjectStream : Stream {
		private readonly IObjectRef objectRef;
		private long position;

		public ObjectStream(IObjectRef objectRef) {
			if (objectRef == null)
				throw new ArgumentNullException("objectRef");

			this.objectRef = objectRef;
		}

		public override void Flush() {
		}

		public override long Seek(long offset, SeekOrigin origin) {
			throw new NotImplementedException();
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			// TODO: intermediate buffer ...
			return objectRef.Read(position, buffer, count);
		}

		public override void Write(byte[] buffer, int offset, int count) {
			// TODO: Intermediate buffer ...
			objectRef.Write(position, buffer, count);
		}

		public override bool CanRead {
			get { return objectRef.IsComplete; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return !objectRef.IsComplete; }
		}

		public override long Length {
			get { return objectRef.RawSize; }
		}

		public override long Position {
			get { return position; }
			set { position = Seek(value, SeekOrigin.Begin); }
		}
	}
}