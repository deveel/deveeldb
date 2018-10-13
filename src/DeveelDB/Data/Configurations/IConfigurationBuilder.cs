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

namespace Deveel.Data.Configurations {
	/// <summary>
	/// Provides a utility to dynamically build a configuration object
	/// </summary>
	public interface IConfigurationBuilder {
		/// <summary>
		/// Set a single entry in the configuration
		/// </summary>
		/// <param name="key">The key of the entry to set</param>
		/// <param name="value">The value of the entry to set</param>
		/// <returns>
		/// Returns an instance of this <see cref="IConfigurationBuilder"/>
		/// that includes the set of the entry
		/// </returns>
		/// <exception cref="ArgumentNullException">If the provided <paramref name="key"/>
		/// is <c>null</c></exception>
		IConfigurationBuilder WithSetting(string key, object value);


		IConfigurationBuilder WithSection(string key, Action<IConfigurationBuilder> configuration);

		/// <summary>
		/// Constructs a new configuration object using
		/// the information collected by this builder-
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IConfiguration"/> that
		/// provides the configurations collected from this builder
		/// </returns>
		IConfiguration Build();
	}
}
