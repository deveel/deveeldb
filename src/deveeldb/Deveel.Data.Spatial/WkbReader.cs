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

namespace Deveel.Data.Spatial {
	public sealed class WkbReader : IGeometryReader {
		public WkbReader(IGeometryFactory factory) {
			Factory = factory;
		}

		public IGeometryFactory Factory { get; private set; }

		GeometryFormat IGeometryReader.Format {
			get { return GeometryFormat.WellKnownBinary; }
		}

		IGeometry IGeometryReader.Read(object input) {
			throw new NotImplementedException();
		}

		public IGeometry Read(Stream stream) {
			var reader = new BinaryReader(stream);
			return Read(reader);
		}

		public IGeometry Read(byte[] bytes) {
			using (var stream = new MemoryStream(bytes)) {
				return Read(stream);
			}
		}

		public IGeometry Read(BinaryReader reader) {
			throw new NotImplementedException();
		}
	}
}
