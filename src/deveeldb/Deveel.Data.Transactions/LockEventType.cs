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

namespace Deveel.Data.Transactions {
	public enum LockEventType {
		/// <summary>
		/// A temporary lock was acquired on a a set of resources
		/// </summary>
		Enter = 1,

		/// <summary>
		/// A temporary lock was released from a set of resources
		/// </summary>
		Exit = 2,

		/// <summary>
		/// An explicit locking happened over a given set of resource
		/// </summary>
		Lock = 3,

		/// <summary>
		/// An existing explicit lock was released (manually or from the expiration
		/// of the transaction that acquired the lock).
		/// </summary>
		Release = 4
	}
}
