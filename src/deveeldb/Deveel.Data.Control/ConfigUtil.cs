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
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data.Control {
	/// <summary>
	/// Provides utilities for retrieving configuration
	/// values from an object.
	/// </summary>
	internal static class ConfigUtil {
		/// <summary>
		/// A dictionary used to cache the pairs of the storage
		/// systems names with their <see cref="StorageType"/>.
		/// </summary>
		private static readonly Dictionary<string, StorageType> cache = new Dictionary<string, StorageType>(30);

		/// <summary>
		/// Gets the type of storage for the system defined in the
		/// configuration given.
		/// </summary>
		/// <param name="config">The <see cref="IDbConfig">database 
		/// configuration object</see> that defines the storage system 
		/// for which to retrieve its kind of storage.</param>
		/// <returns>
		/// Returns a <see cref="StorageType"/> defined by the storage
		/// system configured.
		/// </returns>
		/// <seealso cref="GetStorageType(string)"/>
		public static StorageType GetStorageType(IDbConfig config) {
			string typeName = config.GetValue(ConfigKeys.StorageSystem);
			if (typeName == null)
				throw new InvalidOperationException("A storage system must be specified.");

			return GetStorageType(typeName);
		}

		public static StorageType GetStorageType(string typeName) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException("typeName");

			StorageType storageType;
			if (!cache.TryGetValue(typeName, out storageType)) {
				// in case we're using the internal storage system aliases
				if (String.Compare(typeName, "v1file", true) == 0)
					storageType = StorageType.File;
				else if (String.Compare(typeName, "v1heap", true) == 0)
					storageType = StorageType.Memory;
				else {
					Type type = Type.GetType(typeName, false, true);
					if (type == null)
						throw new InvalidOperationException("The storage system type '" + typeName + "' was not found.");

					if (!typeof(IStoreSystem).IsAssignableFrom(type))
						throw new InvalidOperationException("The type '" + type + "' is not assignable from '" + typeof(IStoreSystem) +
						                                    "'.");

					IStoreSystem storeSystem;

					try {
						storeSystem = (IStoreSystem) Activator.CreateInstance(type, true);
					} catch(Exception e) {
						throw new InvalidOperationException("An error occurred while initializing the type '" + type + "': " + e.Message);
					}

					storageType = storeSystem.StorageType;
				}

				cache[typeName] = storageType;
			}

			return storageType;
		}

		public static bool GetBooleanValue(IDbConfig config, string propertyKey, bool defaultValue) {
			String v = config.GetValue(propertyKey);
			return v == null ? defaultValue : String.Compare(v.Trim(), "enabled", true) == 0;
		}

		public static string GetStringValue(IDbConfig config, string propertyKey, string defaultValue) {
			String v = config.GetValue(propertyKey);
			return v == null ? defaultValue : v.Trim();
		}

		public static int GetIntegerValue(IDbConfig config, string property, int defaultValue) {
			String v = config.GetValue(property);
			return v == null ? defaultValue : Int32.Parse(v);
		}

		/// <summary>
		/// Parses a file string to an absolute position in the file system.
		/// </summary>
		/// <remarks>
		/// We must provide the path to the root directory (eg. the directory 
		/// where the config bundle is located).
		/// </remarks>
		internal static string ParseFileString(string root_path, String root_info, String path_string) {
			string path = Path.GetFullPath(path_string);
			string res;
			// If the path is absolute then return the absoluate reference
			if (Path.IsPathRooted(path_string)) {
				res = path;
			} else {
				// If the root path source is the environment then just return the path.
				if (root_info != null &&
				    root_info.Equals("env")) {
					return path;
				}
					// If the root path source is the configuration file then
					// concat the configuration path with the path string and return it.
				else {
					res = Path.Combine(root_path, path_string);
				}
			}
			return res;
		}
	}
}