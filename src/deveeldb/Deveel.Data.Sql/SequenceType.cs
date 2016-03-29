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

namespace Deveel.Data.Sql {
	/// <summary>
	/// The form of a <see cref="ISequence"/> object in a transaction.
	/// </summary>
	/// <seealso cref="ISequence"/>
	/// <seealso cref="ISequence.SequenceInfo"/>
	/// <seealso cref="SequenceInfo.Type"/>
	public enum SequenceType {
		/// <summary>
		/// A sequence on a table that is handled natively
		/// by the system. Typically, this is the unique incremental
		/// number of record entries in a table.
		/// </summary>
		Native = 1,

		/// <summary>
		/// Denotes a sequence created by the user and that has
		/// a specified incremental factor, and other attributes.
		/// </summary>
		Normal = 2
	}
}
