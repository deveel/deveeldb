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

using Deveel.Data.Configuration;

namespace Deveel.Data.Store.Journaled {
	public sealed class BufferManager : IBufferManager, IConfigurable {
		public void Dispose() {
			throw new NotImplementedException();
		}

		public IJournaledResource CreateResource(string resourceName) {
			throw new NotImplementedException();
		}

		public int Read(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Write(IJournaledResource data, long position, byte[] buffer, int offset, int length) {
			throw new NotImplementedException();
		}

		public void Lock() {
			throw new NotImplementedException();
		}

		public void Release() {
			throw new NotImplementedException();
		}

		public void Checkpoint() {
			throw new NotImplementedException();
		}

		void IConfigurable.Configure(IConfiguration config) {
			throw new NotImplementedException();
		}
	}
}
