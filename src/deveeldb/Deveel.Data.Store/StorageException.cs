// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data;
using Deveel.Data.Diagnostics;

namespace Deveel.Data.Store {
	public class StorageException : ErrorException {
		public StorageException(int errorCode) 
			: this(errorCode, null) {
		}

		public StorageException(int errorCode, string message) 
			: this(errorCode, message, null) {
		}

		public StorageException() 
			: this(null) {
		}

		public StorageException(string message) 
			: this(message, null) {
		}

		public StorageException(string message, Exception innerException) 
			: this(SystemErrorCodes.UnknownStorageError, message, innerException) {
		}

		public StorageException(int errorCode, string message, Exception innerException) 
			: base(errorCode, message, innerException) {
		}
	}
}