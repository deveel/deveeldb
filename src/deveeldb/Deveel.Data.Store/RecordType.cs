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

namespace Deveel.Data.Store {
	/// <summary>
	/// Lists the types of records in a store
	/// </summary>
	public static class RecordType {
		/// <summary>
		/// A record that was added but not commited.
		/// </summary>
		public const byte Added = 0x010;

		/// <summary>
		/// A record that was removed.
		/// </summary>
		public const byte Removed = 0x020;

		/// <summary>
		/// The flag to mark a record state was committed.
		/// </summary>
		public const byte Committed = 0x0F0;
	}
}