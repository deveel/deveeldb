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
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

using Deveel.Data.Protocol;
using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// This object compliments <see cref="ObjectTransfer"/> and provides a 
	/// method to translate any object into a type the database engine can process.
	/// </summary>
	public static class ObjectTranslator {
		/// <summary>
		/// Translates the given object to a type the database can process.
		/// </summary>
		/// <param name="ob"></param>
		/// <returns></returns>
		public static Object Translate(Object ob) {
			if (ob == null)
				return null;
			if (ob is string)
				return StringObject.FromString((String)ob);
			if (ob is StringObject ||
				ob is BigNumber ||
				ob is DateTime ||
				ob is ByteLongObject ||
				ob is bool ||
				ob is StreamableObject)
				return ob;
			if (ob is byte[])
				return new ByteLongObject((byte[])ob);
			if (Attribute.IsDefined(ob.GetType(), typeof(SerializableAttribute)))
				return Serialize(ob);
			throw new ApplicationException("Unable to translate object.  " +
										   "It is not a primitive type or serializable.");
		}

		///<summary>
		/// Serializes the object to a <see cref="ByteLongObject"/>.
		///</summary>
		///<param name="ob"></param>
		///<returns></returns>
		///<exception cref="ApplicationException"></exception>
		public static ByteLongObject Serialize(Object ob) {
			try {
				MemoryStream bout = new MemoryStream();
				BinaryFormatter formatter = new BinaryFormatter();
				formatter.Serialize(bout, ob);
				return new ByteLongObject(bout.ToArray());
			} catch (IOException e) {
				throw new ApplicationException("Serialization error: " + e.Message);
			}
		}

		/// <summary>
		/// Deserializes a <see cref="ByteLongObject"/> to an object.
		/// </summary>
		/// <param name="blob"></param>
		/// <returns></returns>
		public static Object Deserialize(ByteLongObject blob) {
			if (blob == null)
				return null;
			try {
				MemoryStream bin = new MemoryStream(blob.ToArray());
				BinaryFormatter formatter = new BinaryFormatter();
				return formatter.Deserialize(bin);
			} catch (TypeLoadException e) {
				throw new ApplicationException("Type not found: " + e.Message);
			} catch (IOException e) {
				throw new ApplicationException("De-serialization error: " + e.Message);
			}
		}
	}
}