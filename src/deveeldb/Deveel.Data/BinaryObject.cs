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
using System.Runtime.Serialization.Formatters.Binary;

using Deveel.Data.Types;

namespace Deveel.Data {
	[Serializable]
	public sealed class BinaryObject : DataObject {
		private readonly byte[] bytes;

		public static readonly BinaryObject Null = new BinaryObject(PrimitiveTypes.Binary(), (byte[])null);

		public BinaryObject(BinaryType type, byte[] bytes, int offset, int length)
			: base(type) {
			if (bytes == null) {
				this.bytes = null;
			} else {
				this.bytes = new byte[length];
				Array.Copy(bytes, offset, this.bytes, 0, length);
			}
		}

		public BinaryObject(BinaryType type, byte[] bytes)
			: this(type, bytes, 0, bytes != null ? bytes.Length : 0) {
		}

		public BinaryObject(BinaryType type, Stream inputStream)
			: base(type) {
			if (inputStream == null ||
			    inputStream == Stream.Null) {
				bytes = null;
			} else {
				int length = (int) inputStream.Length;
				bytes = new byte[length];
				int i = 0;
				while (i < length) {
					int read = inputStream.Read(bytes, i, length - i);
					if (read == 0)
						throw new EndOfStreamException();

					i += read;
				}
			}
		}

		public override bool IsNull {
			get { return bytes == null; }
		}

		public int Length {
			get { return bytes == null ? 0 : bytes.Length; }
		}

		public byte[] ToArray() {
			if (bytes == null)
				return new byte[0];

			var copyBytes = new byte[bytes.Length];
			Array.Copy(bytes, 0, copyBytes, 0, bytes.Length);
			return copyBytes;
		}

		public Stream GetInputStream() {
			if (bytes == null)
				return Stream.Null;

			return new MemoryStream(bytes,false);
		}

		public object Deserialize() {
			if (bytes == null)
				return null;

			using (var inputStream = GetInputStream()) {
				var formatter = new BinaryFormatter();
				return formatter.Deserialize(inputStream);
			}
		}

		public static BinaryObject Serialize(object obj) {
			if (obj == null)
				return Null;

			using (var stream = new MemoryStream()) {
				var formatter = new BinaryFormatter();
				formatter.Serialize(stream, obj);
				stream.Flush();

				stream.Seek(0, SeekOrigin.Begin);
				var bytes = stream.ToArray();
				return new BinaryObject(PrimitiveTypes.Binary(), bytes);
			}
		}
	}
}
