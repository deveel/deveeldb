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

namespace Deveel.Data.Configuration {
	public sealed class FileConfigSource : IConfigSource {
		public FileConfigSource(string filePath) {
			if (filePath == null)
				throw new ArgumentNullException("filePath");

			FilePath = filePath;
		}

		public string FilePath { get; private set; }

		public Stream InputStream {
			get {
				try {
					return new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024);
				} catch (Exception ex) {
					throw new DatabaseConfigurationException(String.Format("Cannot open a read stream from file '{0}'", FilePath), ex);
				}
			}
		}

		public Stream OutputStream {
			get {
				try {
					return new FileStream(FilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, 1024);
				} catch (Exception ex) {
					throw new DatabaseConfigurationException(String.Format("Cannot open a write stream to file '{0}'", FilePath));
				}
			}
		}
	}
}