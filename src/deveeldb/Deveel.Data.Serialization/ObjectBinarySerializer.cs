// 
//  Copyright 2010-2015 Deveel
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
using System.IO;
using System.Text;

namespace Deveel.Data.Serialization {
	public abstract class ObjectBinarySerializer<T> : IObjectBinarySerializer {
		protected ObjectBinarySerializer() 
			: this(Encoding.Unicode) {
		}

		protected ObjectBinarySerializer(Encoding encoding) {
			if (encoding == null)
				throw new ArgumentNullException("encoding");

			Encoding = encoding;
		}

		protected virtual Encoding Encoding { get; private set; }

		void IObjectSerializer.Serialize(object obj, Stream outputStream) {
			using (var writer = new BinaryWriter(outputStream)) {
				Serialize((T)obj, writer);
			}
		}

		object IObjectSerializer.Deserialize(Stream inputStream) {
			using (var reader = new BinaryReader(inputStream)) {
				return Deserialize(reader);
			}
		}

		void IObjectBinarySerializer.Serialize(object obj, BinaryWriter writer) {
			Serialize((T)obj, writer);
		}

		object IObjectBinarySerializer.Deserialize(BinaryReader reader) {
			return Deserialize(reader);
		}

		public abstract void Serialize(T obj, BinaryWriter writer);

		public abstract T Deserialize(BinaryReader reader);
	}
}
