// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Configurations {
	public sealed class FileConfigurationSource : IConfigurationSource {
		public FileConfigurationSource(string fileName) {
			if (String.IsNullOrEmpty(fileName))
				throw new ArgumentNullException(nameof(fileName));

			FileName = fileName;
		}

		public string FileName { get; }

		Stream IConfigurationSource.InputStream => new FileStream(FileName, FileMode.Open, FileAccess.Read, FileShare.Read);
	}
}