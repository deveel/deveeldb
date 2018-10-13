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
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Deveel.Data.Configurations {
	public sealed class StringConfigurationSource : IConfigurationSource {
		public StringConfigurationSource(string source) {
			Source = source;
		}

		public string Source { get; }

		Stream IConfigurationSource.InputStream {
			get {
				var bytes = Encoding.UTF8.GetBytes(Source);
				return new MemoryStream(bytes);
			}
		}
	}
}