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
	public sealed class AreaStream : Stream {
		private readonly IArea area;

		public AreaStream(IArea area) {
			if (area == null)
				throw new ArgumentNullException("area");

			this.area = area;
		}

		public override void Flush() {
			area.Flush();
		}

		public override long Seek(long offset, SeekOrigin origin) {
			if (origin != SeekOrigin.Begin)
				throw new NotImplementedException();

			area.Position = (int) offset;
			return area.Position;
		}

		public override void SetLength(long value) {
			throw new NotSupportedException();
		}

		public override int Read(byte[] buffer, int offset, int count) {
			return area.Read(buffer, offset, count);
		}

		public override void Write(byte[] buffer, int offset, int count) {
			area.Write(buffer, offset, count);
		}

		public override bool CanRead {
			get { return true; }
		}

		public override bool CanSeek {
			get { return true; }
		}

		public override bool CanWrite {
			get { return !area.IsReadOnly; }
		}

		public override long Length {
			get { return area.Capacity; }
		}

		public override long Position {
			get { return area.Position; }
			set { Seek(value, SeekOrigin.Begin); }
		}
	}
}