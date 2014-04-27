// 
//  Copyright 2011  Deveel
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

namespace Deveel.Data.Index {
	/// <summary>
	/// An interface for querying and accessing an index of primitive integers.
	/// </summary>
	/// <remarks>
	/// The index may or may not be sorted or may be sorted over an 
	/// <see cref="IIndexComparer"/>.
	/// <para>
	/// This interface exposes general index querying/inserting/removing methods.
	/// </para>
	/// <para>
	/// How the index is physically stored is dependant on the implementation of
	/// the interface.
	/// </para>
	/// <para>
	/// An example of an implementation is <see cref="BlockIndex"/>.
	/// </para>
	/// </remarks>
	public interface IIndex : IIndex<int> {
	}
}