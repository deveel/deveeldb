// 
//  Copyright 2010-2013  Deveel
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

namespace Deveel.Data.Control {
	/// <summary>
	/// This static class provides default configuration values
	/// that can be set in a <see cref="DbConfig"/> instance,
	/// in association with keys defined in <see cref="ConfigKeys"/>
	/// to configure a database system.
	/// </summary>
	/// <remarks>
	/// The values of the fields in the class are simply a formal
	/// definition and they are not intended to be constraining
	/// in the free configuration of database system.
	/// </remarks>
	public static class ConfigValues {
		/// <summary>
		/// Associated to the key <see cref="ConfigKeys.StorageSystem"/>
		/// tells the system the database storage will be backed by the 
		/// file-system (default implementation)
		/// </summary>
		public const string FileStorageSystem = "file";

		/// <summary>
		/// Associated to the key <see cref="ConfigKeys.StorageSystem"/>
		/// tells the system the database storage will be backed by the 
		/// heap (default implementation)
		/// </summary>
		public const string HeapStorageSystem = "heap";

		public const string HeapCache = "heap";

		public const string SystemRegexLibrary = "Deveel.Data.Text.SystemRegexLibrary";
	}
}
