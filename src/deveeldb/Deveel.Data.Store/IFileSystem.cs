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

namespace Deveel.Data.Store {
	public interface IFileSystem {
		bool FileExists(string path);

		IFile OpenFile(string path, bool readOnly);

		IFile CreateFile(string path);

		bool DeleteFile(string path);

		string CombinePath(string path1, string path2);

		bool RenameFile(string sourcePath, string destPath);

		bool DirectoryExists(string path);

		void CreateDirectory(string path);

		long GetFileSize(string path);
	}
}
