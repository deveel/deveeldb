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

namespace Deveel.Data.Configuration {
	/// <summary>
	/// A source for stored configurations or destination
	/// to configurations to store.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The streams returned from implementations of this interface
	/// will be passed to <see cref="IConfigFormatter"/>
	/// instances for constructing a <see cref="IConfiguration"/> object,
	/// or to store the configurations of a <see cref="IConfiguration"/>
	/// into a given output.
	/// </para>
	/// </remarks>
	public interface IConfigSource {
		/// <summary>
		/// Gets a <see cref="Stream"/> that is used to load the
		/// configurations.
		/// </summary>
		Stream InputStream { get; }

		/// <summary>
		/// Gets a <see cref="Stream"/> that can be writtern with
		/// the configurations from a <see cref="IConfiguration"/>.
		/// </summary>
		Stream OutputStream { get; }
	}
}