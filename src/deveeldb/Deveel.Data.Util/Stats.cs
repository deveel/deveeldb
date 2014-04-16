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
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;

namespace Deveel.Data {
	///<summary>
	/// An object that is used to store and update various stats.
	///</summary>
	/// <remarks>
	/// <b>Note</b>: This object is thread safe.
	/// </remarks>
	public sealed class Stats {

		/// <summary>
		/// Where the stat properties are held.
		/// </summary>
		private readonly IDictionary<string, IntegerStat> properties;

		///<summary>
		///</summary>
		public Stats() {
			// We need lookup on this hash to be really quick, so load factor is
			// low and initial capacity is high.
			properties = new Dictionary<string, IntegerStat>(250);
		}

		///<summary>
		/// Resets all stats that start with "{session}" to 0.
		///</summary>
		/// <remarks>
		/// This should be called when we are collecting stats 
		/// over a given session and a session has finished.
		/// </remarks>
		public void ResetSession() {
			lock (this) {
				var keys = new string[properties.Count];
				properties.Keys.CopyTo(keys, 0);

				// If key starts with a "{session}" then reset it to 0.
				for (int i = 0; i < keys.Length; ++i) {
					if (keys[i].StartsWith("{session}")) {
						IntegerStat stat = properties[keys[i]];
						stat.value = 0;
					}
				}
			}
		}

		/// <summary>
		///  Adds the given value to a stat property.
		/// </summary>
		/// <param name="statName"></param>
		/// <param name="value"></param>
		public void Add(string statName, int value) {
			lock (this) {
				IntegerStat stat;
				if (properties.TryGetValue(statName, out stat)) {
					stat.value += value;
				} else {
					stat = new IntegerStat {value = value};
					properties[statName] = stat;
				}
			}
		}

		///<summary>
		/// Increments a stat property.
		///</summary>
		///<param name="statName"></param>
		public void Increment(String statName) {
			lock (this) {
				IntegerStat stat;
				if (properties.TryGetValue(statName, out stat)) {
					++stat.value;
				} else {
					stat = new IntegerStat {value = 1};
					properties[statName] = stat;
				}
			}
		}

		///<summary>
		/// Decrements a stat property.
		///</summary>
		///<param name="statName"></param>
		public void Decrement(String statName) {
			lock (this) {
				IntegerStat stat;
				if (properties.TryGetValue(statName, out stat)) {
					--stat.value;
				} else {
					stat = new IntegerStat {value = -1};
					properties[statName] = stat;
				}
			}
		}

		///<summary>
		/// Retrieves the current Object value of a stat property or null 
		/// if the stat wasn't found.
		///</summary>
		///<param name="statName"></param>
		///<returns></returns>
		public object Get(String statName) {
			lock (this) {
				IntegerStat stat;
				return properties.TryGetValue(statName, out stat) ? (object) stat.value : null;
			}
		}

		/// <summary>
		///  Sets the given stat name with the given value.
		/// </summary>
		/// <param name="statName"></param>
		/// <param name="value"></param>
		public void Set(string statName, int value) {
			lock (this) {
				IntegerStat stat;
				if (properties.TryGetValue(statName, out stat)) {
					stat.value = value;
				} else {
					stat = new IntegerStat();
					stat.value = value;
					properties[statName] = stat;
				}
			}
		}

		///<summary>
		/// Return a String array of all stat keys sorted in order from lowest to highest.
		///</summary>
		public string[] Keys {
			get {
				lock (this) {
					var keys = new String[properties.Count];
					properties.Keys.CopyTo(keys, 0);

					// Sort the keys
					Array.Sort(keys, StringComparer.InvariantCulture);

					return keys;
				}
			}
		}


		///<summary>
		/// Returns a String representation of the stat with the given key name.
		///</summary>
		///<param name="key"></param>
		///<returns></returns>
		public String StatString(String key) {
			lock (this) {
				IntegerStat stat;
				if (!properties.TryGetValue(key, out stat))
					return String.Empty;

				return stat.value.ToString(CultureInfo.InvariantCulture);
			}
		}

		public override String ToString() {
			lock (this) {
				var buf = new StringBuilder();
				foreach (var property in properties) {
					IntegerStat stat = property.Value;
					buf.Append(property.Key);
					buf.Append(": ");
					buf.Append(stat.value);
					buf.Append('\n');
				}

				return buf.ToString();
			}
		}

		///<summary>
		/// Outputs the stats to a print stream.
		///</summary>
		///<param name="output"></param>
		public void PrintTo(TextWriter output) {
			lock (this) {
				foreach (var property in properties) {
					output.Write(property.Key);
					output.Write(": ");
					output.WriteLine(property.Value.value);
				}
			}
		}

		// ---------- Inner class ----------

		private sealed class IntegerStat {
			internal long value;
		}
	}
}