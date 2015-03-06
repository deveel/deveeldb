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

namespace Deveel.Data.Configuration {
	/// <summary>
	/// Marks a component as configurable and passes the configuration
	/// obejct that is used to load the configurations handled.
	/// </summary>
	public interface IConfigurable {
		/// <summary>
		/// Configures the component with the settings provided
		/// by the specified configuration object.
		/// </summary>
		/// <param name="config">The container of settings used to
		/// configure the object</param>
		/// <seealso cref="IDbConfig"/>
		void Configure(IDbConfig config);
	}
}