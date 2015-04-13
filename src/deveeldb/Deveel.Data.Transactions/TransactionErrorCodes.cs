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

namespace Deveel.Data.Transactions {
	public static class TransactionErrorCodes {
		public const int TableRemoveClash = 0x0223005;
		public const int RowRemoveClash = 0x00981100;
		public const int TableDropped = 0x01100200;
		public const int DuplicateTable = 0x0300210;
		public const int ReadOnly = 0x00255631;
		public const int DirtySelect = 0x007811920;
	}
}
