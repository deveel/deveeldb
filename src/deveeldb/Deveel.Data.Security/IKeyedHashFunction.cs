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

namespace Deveel.Data.Security {
	/// <summary>
	/// An <see cref="IHashFunction">hash function</see> that requires a private
	/// key to compute the final result.
	/// </summary>
	public interface IKeyedHashFunction : IHashFunction {
		/// <summary>
		/// Gets or sets the private key to use for computing the hash.
		/// </summary>
		byte[] Key { get; set; }
	}
}