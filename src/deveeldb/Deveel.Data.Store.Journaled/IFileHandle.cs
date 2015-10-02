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

namespace Deveel.Data.Store.Journaled {
	public interface IFileHandle : IDisposable {
		string FileName { get; }

		bool IsReadOnly { get; }

		long Position { get; }

		long Length { get; }


		long Seek(long offset, SeekOrigin origin);


		int Read(byte[] buffer, int offset, int length);

		void Write(byte[] buffer, int offset, int length);

		void Flush(bool writeThrough);

		void Close();

		void Delete();

		void SetLength(long value);
	}
}
