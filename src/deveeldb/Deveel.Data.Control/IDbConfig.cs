// 
//  Copyright 2010  Deveel
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
using System.Collections;
using System.IO;

namespace Deveel.Data.Control {
	///<summary>
	/// A container object of configuration details of a database system.
	///</summary>
	/// <remarks>
	/// This object can be used to programmatically setup configuration 
	/// properies in a database system.
	/// </remarks>
	public interface IDbConfig : ICloneable, IEnumerable {
		///<summary>
		/// Returns the current path set for this configuration.
		///</summary>
		/// <remarks>
		/// This is useful if the configuration is based on a configuration 
		/// file that has path references relative to the configuration file. 
		/// In this case, the path returned here would be the path to the 
		/// configuration file.
		/// </remarks>
		string CurrentPath { get; }

		///<summary>
		/// Returns the value that was set for the configuration property 
		/// with the given name.
		///</summary>
		///<param name="property_key"></param>
		/// <remarks>
		/// This method must always returns a value that the database engine 
		/// can use provided the 'property_key' is a supported key.  If the 
		/// property key is not supported and the key was not set, null is returned.
		/// </remarks>
		///<returns></returns>
		string GetValue(string property_key);

		/// <summary>
		/// Sets the value for the configuration property with the
		/// given name.
		/// </summary>
		/// <param name="property">The name of the configuration property 
		/// to set.</param>
		/// <param name="value">The string value to set to the property.</param>
		/// <remarks>
		/// If a property with the same name was already set, calling this method
		/// will change the value of the property given.
		/// </remarks>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="property"/> name provided is <b>null</b> or empty.
		/// </exception>
		void SetValue(string property, string value);

		/// <summary>
		/// Merges the current configurations with the ones contained in
		/// the given <paramref name="config">configuration</paramref>.
		/// </summary>
		/// <param name="config">The source configuration with which
		/// to merge the current configuration.</param>
		/// <remarks>
		/// This method takes in considerations only the values not
		/// already defined in the current configuration.
		/// This means that if the key <c>name</c> is defined in the current
		/// configuration and it's defined also in <paramref name="config"/>,
		/// the second one won't be taken in consideration during the merge.
		/// </remarks>
		/// <returns>
		/// Returns the current instance of the <see cref="IDbConfig"/>
		/// resulting from the merge with the given <paramref name="config"/>.
		/// </returns>
		IDbConfig Merge(IDbConfig config);

		/// <summary>
		/// Saves the configurations in this object to the stream given.
		/// </summary>
		/// <param name="stream">The destination stream where to save the
		/// configurations contained in this object.</param>
		/// <exception cref="ArgumentException">
		/// If the destination <paramref name="stream"/> is not writeable.
		/// </exception>
		void SaveTo(Stream stream);
	}
}