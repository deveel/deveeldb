//  
//  IDbConfig.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

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
		/// Saves the configurations in this object to the file located
		/// at the path given.
		/// </summary>
		/// <param name="fileName">The name of the file where to save the
		/// configurations contained in this object.</param>
		/// <remarks>
		/// This method 
		/// </remarks>
		void SaveTo(string fileName);
	}
}