// 
//  Copyright 2010-2018 Deveel
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

namespace Deveel.Data.Configurations {
	/// <summary>
	/// Provides the format to load and store configurations
	/// in and from a stream.
	/// </summary>
	/// <remarks>
	/// Implementations of this interface will apply a specific
	/// format to the data stored and retrieved (for example
	/// an XML fragmanet, a key/value file, a binary file, etc.).
	/// </remarks>
	public interface IConfigurationFormatter {
		/// <summary>
		/// Loads a stored configuration from the given stream
		/// into the configuration argument.
		/// </summary>
		/// <param name="config">The configuration object inside of which
		/// to load the configurations from the the given stream.</param>
		/// <param name="inputStream">The stream from where to read the
		/// configurations formatted into the object provided as argument.</param>
		/// <exception cref="ArgumentException">
		/// If the provided <paramref name="inputStream"/> cannot be read.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If either <paramref name="config"/> or <paramref name="inputStream"/>
		/// are <c>null</c>.
		/// </exception>
		void LoadInto(IConfigurationBuilder config, Stream inputStream);
	}
}