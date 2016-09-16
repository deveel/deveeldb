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
using System.IO;

namespace Deveel.Data.Configuration {
	/// <summary>
	/// Provides the format to load and store configurations
	/// in and from a stream.
	/// </summary>
	/// <remarks>
	/// Implementations of this interface will apply a specific
	/// format to the data stored and retrieved (for example
	/// an XML fragment, a key/value file, a binary file, etc.).
	/// </remarks>
	public interface IConfigurationFormatter {
		/// <summary>
		/// Loads a stored configuration from the given source
		/// into the configuration argument.
		/// </summary>
		/// <param name="config">The configuration object inside of which
		/// to load the configurations from the given source.</param>
		/// <param name="source">The source from where to read the
		/// configurations formatted into the object provided as argument.</param>
		/// <exception cref="ArgumentException">
		/// If the provided <paramref name="source"/> cannot be read.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If either <paramref name="config"/> or <paramref name="source"/>
		/// are <c>null</c>.
		/// </exception>
		void LoadInto(IConfiguration config, IConfigurationSource source);

		/// <summary>
		/// Stores the given level of configurations into the source
		/// provided, in the format handled by this interface.
		/// </summary>
		/// <param name="config">The source of the configurations to store.</param>
		/// <param name="level">The level of the configurations to load from
		/// the source and store.</param>
		/// <param name="source">The destination where the formatter
		/// saves the configurations retrieved from the source.</param>
		/// <seealso cref="ConfigurationLevel"/>
		/// <exception cref="ArgumentException">
		/// If the provided <paramref name="source"/> cannot be written.
		/// </exception>
		/// <exception cref="ArgumentNullException">
		/// If either <paramref name="config"/> or <paramref name="source"/>
		/// are <c>null</c>.
		/// </exception>
		void SaveFrom(IConfiguration config, ConfigurationLevel level, IConfigurationSource source);
	}
}